(ns teensyp.server
  "The main server namespace."
  (:require [teensyp.concurrent :refer [with-lock]])
  (:import [java.io Closeable IOException]
           [java.net InetSocketAddress]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey
            ServerSocketChannel SocketChannel]
           [java.util Queue]
           [java.util.concurrent ArrayBlockingQueue Executors ExecutorService]
           [java.util.concurrent.atomic AtomicInteger]
           [java.util.concurrent.locks ReentrantLock]))
           
(defn- server-socket-channel ^ServerSocketChannel [port]
  (doto (ServerSocketChannel/open)
    (.configureBlocking false)
    (.bind (InetSocketAddress. port))))

(defn- server-selector [^ServerSocketChannel server-ch]
  (let [selector (Selector/open)]
    (.register server-ch selector SelectionKey/OP_ACCEPT)
    selector))

(defn- foreach! [f ^Iterable coll]
  (let [iter (.iterator coll)]
    (loop []
      (when (.hasNext iter)
        (f (.next iter))
        (.remove iter)
        (recur)))))

;; The socket channel is only read from when no flags are set. That is, the
;; channel is not closed or paused, and not in the middle of writing or
;; still in the middle of handling a previous read (working).

(def ^:private ^:const writing 0x01)
(def ^:private ^:const working 0x02)
(def ^:private ^:const closed  0x04)
(def ^:private ^:const paused  0x08)

(defn- bit-flag-set? [flags flag]
  (not (zero? (bit-and flags flag))))

(defn- interest-ops [flags]
  (bit-or (if (zero? flags) SelectionKey/OP_READ 0)
          (if (and (bit-flag-set? flags writing)
                   (not (bit-flag-set? flags closed)))
            SelectionKey/OP_WRITE 0)))

(defn- update-flags [^SelectionKey key f]
  (let [vflags (-> key .attachment :flags)]
    (with-lock (-> key .attachment :lock)
      (let [flags (vswap! vflags f)]
        (when (.isValid key)
          (.interestOps key (interest-ops flags)))
        flags))))

(defn- set-flag [key flag]
  (update-flags key #(bit-or % flag)))

(defn- unset-flag [key flag]
  (update-flags key #(bit-and-not % flag)))

(defn- has-flag? [^SelectionKey key flag]
  (-> key .attachment :flags deref (bit-flag-set? flag)))

(defprotocol Socket
  "A protocol representing a client socket. See also: [[write]], [[close]],
  [[pause-reads]] and [[resume-reads]]."
  (queue-write [socket buffer callback]
    "Queue the buffer to be written to the socket. If the callback is not nil,
    it will be called as a zero argument function once the buffer has been
    written. See [[write]] for a more convenient way of calling this method.")
  (socket-info [socket]
    "Return a map of information about the socket's connection. This includes
    keys for :local-address and :remote-address, which are immutable
    java.net.InetSocketAddress instances."))

(defn write
  "Queue up a ByteBuffer to be written a socket defined by the Socket protocol.
  Accepts an optional, zero argument callback function that will be run after
  the buffer has been written."
  ([socket buffer]          (queue-write socket buffer nil))
  ([socket buffer callback] (queue-write socket buffer callback)))

(defn close
  "Queue the supplied Socket to be closed. Accepts an optional, zero argument
  callback that will be run after the socket has been closed."
  ([socket]          (queue-write socket ::close nil))
  ([socket callback] (queue-write socket ::close callback)))

;; We send pause/resume events to the write queue. This ensures they execute
;; in the same thread as the server selector, wakes the selector up, and
;; provides a callback. While we could create a separate system for handling
;; pause/resume, it's probably not worth the overhead.

(defn pause-reads
  "Pause reads for this Socket. See: [[resume-reads]]."
  ([socket]          (queue-write socket ::pause-reads nil))
  ([socket callback] (queue-write socket ::pause-reads callback)))

(defn resume-reads
  "Resume reads for this Socket. See: [[pause-reads]]."
  ([socket]          (queue-write socket ::resume-reads nil))
  ([socket callback] (queue-write socket ::resume-reads callback)))

(defn- ex-write-queue-full []
  (ex-info "Write queue full" {:err ::write-queue-full}))

(defn- ex-write-queue-over-capacity []
  (ex-info "Write queue over capacity" {:err ::write-queue-over-capacity}))

(defn- update-write-limit [^AtomicInteger limit ^ByteBuffer buffer]
  (let [remaining (.remaining buffer)]
    (when (> remaining (.getAndAdd limit (- remaining)))
      (.getAndAdd limit remaining)
      (throw (ex-write-queue-over-capacity)))))

(extend-protocol Socket
  SelectionKey
  (queue-write [key buffer callback]
    (let [{:keys [^ArrayBlockingQueue write-queue write-limit]}
          (.attachment key)]
       (when (zero? (.remainingCapacity write-queue))
         (throw (ex-write-queue-full)))
       (when (instance? ByteBuffer buffer)
         (update-write-limit write-limit buffer))
       (.add write-queue [buffer callback])
       (set-flag key writing)
       (-> key .selector .wakeup)))
  (socket-info [key]
    (-> key .attachment :socket-info)))

(defn- new-context
  [^SocketChannel ch
   {:keys [read-buffer-size write-buffer-size write-queue-size]
    :or   {read-buffer-size  8192
           write-buffer-size 32768
           write-queue-size  64}}]
  {:write-queue (ArrayBlockingQueue. write-queue-size)
   :write-limit (AtomicInteger. write-buffer-size)
   :lock        (ReentrantLock.)
   :flags       (volatile! 0)
   :state       (volatile! nil)
   :read-buffer (ByteBuffer/allocate read-buffer-size)
   :closef      (volatile! nil)
   :socket-info {:local-address  (.getLocalAddress ch)
                 :remote-address (.getRemoteAddress ch)}})

(defn- close-key [^SelectionKey key submit ex handler]
  (let [state (-> key .attachment :state)]
    (submit #(vswap! state handler ex))))

(defn- handle-close [^SelectionKey key submit ex {:keys [handler]}]
  (when (instance? SocketChannel (.channel key))
    (let [closef (-> key .attachment :closef)]
      (-> key .channel .close)
      (set-flag key closed)
      (with-lock (-> key .attachment :lock)
        (if (has-flag? key working)
          (vreset! closef #(close-key key submit ex handler))
          (close-key key submit ex handler))))))

(defn- handle-accept [^SelectionKey key submit {:keys [handler] :as opts}]
  (let [^Selector selector (.selector key)
        ^SocketChannel  ch (.accept ^ServerSocketChannel (.channel key))]
    (.configureBlocking ch false)
    (let [{:keys [state] :as context} (new-context ch opts)
          key (.register ch selector 0 context)]
      (set-flag key working)
      (submit #(try (vreset! state (handler key))
                    (catch Exception ex
                      (handle-close key submit ex opts))
                    (finally
                      (let [flags (unset-flag key working)]
                        (if (bit-flag-set? flags closed)
                          (@(-> key .attachment :closef))
                          (.wakeup selector)))))))))

(defn- submit-read-handler [^SelectionKey key submit {:keys [handler] :as opts}]
  (let [{:keys [^ByteBuffer read-buffer state]} (.attachment key)
        ^Selector selector (.selector key)]
    (.flip read-buffer)
    (set-flag key working)
    (submit #(try (vswap! state handler key read-buffer)
                  (catch Exception ex
                    (handle-close key submit ex opts))
                  (finally
                    (.compact read-buffer)
                    (let [flags (unset-flag key working)]
                      (if (bit-flag-set? flags closed)
                        (@(-> key .attachment :closef))
                        (.wakeup selector))))))))

(defn- handle-read [^SelectionKey key submit opts]
  (let [^SocketChannel ch (.channel key)]
    (try
      (if (neg? (.read ch ^ByteBuffer (:read-buffer (.attachment key))))
        (handle-close key submit nil opts)
        (submit-read-handler key submit opts)) 
      (catch IOException ex
        (handle-close key submit ex opts)))))

(defn- handle-resumed [^SelectionKey key submit opts]
  (try
    (when (pos? (.position ^ByteBuffer (:read-buffer (.attachment key))))
      (submit-read-handler key submit opts)) 
    (catch IOException ex
      (handle-close key submit ex opts))))

(defn- resumed? [flags-before flags-after]
  (and (bit-flag-set? flags-before paused)
       (not (bit-flag-set? flags-after paused))))

(defn- handle-write [^SelectionKey key submit opts]
  (let [{:keys [^Queue         write-queue
                ^AtomicInteger write-limit]} (.attachment key)
        ^SocketChannel ch (-> key .channel)
        initial-flags     (-> key .attachment :flags deref)]
    (try (loop []
           (if-some [[buffer callback] (.peek write-queue)]
             (condp identical? buffer
               ::close        (do (.close ch)
                                  (handle-close key submit nil opts)
                                  (some-> callback submit))
               ::pause-reads  (do (set-flag key paused)
                                  (.poll write-queue)
                                  (some-> callback submit)
                                  (recur))
               ::resume-reads (do (unset-flag key paused)
                                  (.poll write-queue)
                                  (some-> callback submit)
                                  (recur))
               (do (.getAndAdd write-limit (.write ch ^ByteBuffer buffer))
                   (when-not (.hasRemaining ^ByteBuffer buffer)
                     (.poll write-queue)
                     (some-> callback submit)
                     (recur))))
             (when (resumed? initial-flags (unset-flag key writing))
               (handle-resumed key submit opts))))
         (catch IOException ex
           (handle-close key submit ex opts)))))

(defn- handle-key [^SelectionKey key submit opts]
  (when (.isValid key)
    (cond
      (.isReadable key)   (handle-read key submit opts)
      (.isAcceptable key) (handle-accept key submit opts)
      (.isWritable key)   (handle-write key submit opts))))

(defn- server-loop
  [^ServerSocketChannel server-ch ^Selector selector
   ^ExecutorService executor opts]
  (letfn [(submit [f] (.submit executor ^Runnable f))]
    (try
      (loop []
        (when (.isOpen server-ch)
          (.select selector)
          (foreach! #(handle-key % submit opts) (.selectedKeys selector))
          (recur)))
      (finally
        (run! #(handle-close % submit nil opts) (.keys selector))
        (.close selector)
        (.shutdown executor)))))

(defn- new-default-executor []
  (let [processors (.availableProcessors (Runtime/getRuntime))]
    (Executors/newFixedThreadPool (+ 2 processors))))

(defn start-server
  "Start a TCP server with the supplied map of options:

  - `:port` - the port number to listen on (mandatory)
  - `:handler` - a handler function (mandatory, see below)
  - `:executor` - a custom ExecutorService to supply worker threads
  - `:read-buffer-size` - the read buffer size in bytes (default 8K)
  - `:write-buffer-size` - the write buffer size in bytes (default 32K)
  - `:write-queue-size` - the max number of writes in the queue (default 64)

  The handler function must have three arities:

      (fn handler
        ([socket] initial-state)           ;; on socket accept
        ([state socket buffer] new-state)  ;; on socket read data
        ([state exception]))               ;; on socket close

  The `buffer` is a java.nio.ByteBuffer instance, and `socket` is an object
  that satisfies the [[Socket]] protocol.

  The `state` is a custom data structure that is returned when the accept or
  read arities are triggered. A different state is associated with each
  connection.

  When closing, the `exception` may contain the exception that terminated the
  channel, or `nil` if the channel were terminated gracefully.

  The handler function is guaranteed to execute in serial per channel. That is,
  the accept will always be first, the close will always be last, and reads
  will always be sequential."
  ^Closeable [{:keys [port executor] :as opts}]
  {:pre [(int? port)]}
  (let [server-ch (server-socket-channel port)
        selector  (server-selector server-ch)
        executor  (or executor (new-default-executor))]
    (.start (Thread. #(server-loop server-ch selector executor opts)))
    server-ch))
