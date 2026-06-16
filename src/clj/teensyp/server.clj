(ns teensyp.server
  "The main server namespace."
  (:require [teensyp.concurrent :refer [with-lock]])
  (:import [java.io Closeable IOException]
           [java.net InetSocketAddress StandardSocketOptions]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey
            ServerSocketChannel SocketChannel]
           [java.util Queue Set]
           [java.util.concurrent ArrayBlockingQueue ConcurrentHashMap
            Executors ExecutorService]
           [java.util.concurrent.atomic AtomicInteger]
           [java.util.concurrent.locks ReentrantLock]))

(defn- server-socket-channel ^ServerSocketChannel [port]
  (doto (ServerSocketChannel/open)
    (.configureBlocking false)
    (.bind (InetSocketAddress. port))))

(defn- server-selector ^Selector [^ServerSocketChannel server-ch]
  (let [selector (Selector/open)]
    (.register server-ch selector SelectionKey/OP_ACCEPT)
    selector))

(defn- foreach! [f ^Iterable coll]
  (let [iter (.iterator coll)]
    (loop []
      (when (.hasNext iter)
        (when (f (.next iter))
          (.remove iter))
        (recur)))))

;; The socket channel is only read from when no flags are set. That is, the
;; channel is not closed or paused, and not in the middle of writing or
;; still in the middle of handling a previous read (working).

(def ^:private ^:const WRITING 0x01)
(def ^:private ^:const WORKING 0x02)
(def ^:private ^:const CLOSED  0x04)
(def ^:private ^:const PAUSED  0x08)

(defn- bit-flag-set? [flags flag]
  (not (zero? (bit-and flags flag))))

(defn- interest-ops [flags]
  (bit-or (if (zero? flags) SelectionKey/OP_READ 0)
          (if (and (bit-flag-set? flags WRITING)
                   (not (bit-flag-set? flags CLOSED)))
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
  (queue-control [socket control callback]
    "Queue a control event, such as ::pause-reads or ::resume-reads.")
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
  the buffer has been written. The maximum number of queued buffers per socket
  is governed by the `:write-queue-size` option, and the maximum size of bytes
  that can be held in *all* queued buffers is `:write-buffer-size`. Exceeding
  either of these limits will throw an ExceptionInfo."
  ([socket buffer]          (queue-write socket buffer nil))
  ([socket buffer callback] (queue-write socket buffer callback)))

(defn close
  "Queue the supplied Socket to be closed. Accepts an optional, zero argument
  callback that will be run after the socket has been closed."
  ([socket]          (queue-write socket ::close nil))
  ([socket callback] (queue-write socket ::close callback)))

(defn pause-reads
  "Pause reads for this Socket. See: [[resume-reads]]. This is a control event,
  and is limited by the `:control-queue-size`. Too many control events queued
  at once for a single Socket will throw an ExceptionInfo."
  ([socket]          (queue-control socket ::pause-reads nil))
  ([socket callback] (queue-control socket ::pause-reads callback)))

(defn resume-reads
  "Resume reads for this Socket. Forces a call to the read handler if any
  data is waiting on the socket buffer. See: [[pause-reads]]. This is a control
  event, and is limited by the `:control-queue-size`. Too many control events
  queued at once for a single Socket will throw an ExceptionInfo."
  ([socket]          (queue-control socket ::resume-reads nil))
  ([socket callback] (queue-control socket ::resume-reads callback)))

(defn- ex-control-queue-full []
  (ex-info "Control queue full" {:err ::control-queue-full}))

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
  (queue-control [key event callback]
    (let [{:keys [^ArrayBlockingQueue control-queue ^Set pending-set]}
          (.attachment key)]
      (when (zero? (.remainingCapacity control-queue))
        (throw (ex-control-queue-full)))
      (.add control-queue [event callback])
      (.add pending-set key)
      (-> key .selector .wakeup)))
  (queue-write [key buffer callback]
    (let [{:keys [^ArrayBlockingQueue write-queue write-limit]}
          (.attachment key)]
      (when (zero? (.remainingCapacity write-queue))
        (throw (ex-write-queue-full)))
      (when (instance? ByteBuffer buffer)
        (update-write-limit write-limit buffer))
      (.add write-queue [buffer callback])
      (set-flag key WRITING)
      (-> key .selector .wakeup)))
  (socket-info [key]
    (-> key .attachment :socket-info)))

(defn- new-context
  [^SocketChannel ch pending-set
   {:keys [control-queue-size read-buffer-size
           write-buffer-size write-queue-size]
    :or   {control-queue-size 32
           read-buffer-size   8192
           write-buffer-size  32768
           write-queue-size   64}}]
  {:write-queue   (ArrayBlockingQueue. write-queue-size)
   :write-limit   (AtomicInteger. write-buffer-size)
   :lock          (ReentrantLock.)
   :flags         (volatile! 0)
   :state         (volatile! nil)
   :read-buffer   (ByteBuffer/allocate read-buffer-size)
   :closef        (volatile! nil)
   :pending-set   pending-set
   :control-queue (ArrayBlockingQueue. control-queue-size)
   :socket-info   {:local-address  (.getLocalAddress ch)
                   :remote-address (.getRemoteAddress ch)}})

(defn- close-key [^SelectionKey key submit ex handler]
  (let [state (-> key .attachment :state)]
    (submit #(vswap! state handler ex))))

(defn- handle-close [^SelectionKey key submit ex {:keys [handler]}]
  (when (instance? SocketChannel (.channel key))
    (let [closef (-> key .attachment :closef)]
      (-> key .channel .close)
      (set-flag key CLOSED)
      (with-lock (-> key .attachment :lock)
        (if (has-flag? key WORKING)
          (vreset! closef #(close-key key submit ex handler))
          (close-key key submit ex handler))))))

(defn- handle-accept
  [^SelectionKey key submit pending {:keys [handler] :as opts}]
  (let [^Selector selector (.selector key)
        ^SocketChannel  ch (.accept ^ServerSocketChannel (.channel key))]
    (.configureBlocking ch false)
    (.setOption ch StandardSocketOptions/TCP_NODELAY true)
    (let [{:keys [state] :as context} (new-context ch pending opts)
          key (.register ch selector 0 context)]
      (set-flag key WORKING)
      (submit #(try (vreset! state (handler key))
                    (catch Exception ex
                      (handle-close key submit ex opts))
                    (finally
                      (let [flags (unset-flag key WORKING)]
                        (if (bit-flag-set? flags CLOSED)
                          (@(-> key .attachment :closef))
                          (.wakeup selector)))))))))

(defn- submit-read-handler [^SelectionKey key submit {:keys [handler] :as opts}]
  (let [{:keys [^ByteBuffer read-buffer state]} (.attachment key)
        ^Selector selector (.selector key)]
    (.flip read-buffer)
    (set-flag key WORKING)
    (submit #(try (vswap! state handler key read-buffer)
                  (catch Exception ex
                    (handle-close key submit ex opts))
                  (finally
                    (.compact read-buffer)
                    (let [flags (unset-flag key WORKING)]
                      (if (bit-flag-set? flags CLOSED)
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

(defn- handle-write [^SelectionKey key submit opts]
  (let [{:keys [^Queue         write-queue
                ^AtomicInteger write-limit]} (.attachment key)
        ^SocketChannel ch (-> key .channel)]
    (unset-flag key WRITING)
    (try (loop []
           (when-some [[buffer callback] (.peek write-queue)]
             (if (identical? buffer ::close)
               (do (.close ch)
                   (handle-close key submit nil opts)
                   (some-> callback submit))
               (do (.getAndAdd write-limit (.write ch ^ByteBuffer buffer))
                   (if (.hasRemaining ^ByteBuffer buffer)
                     (set-flag key WRITING)    ;; partial write
                     (do (.poll write-queue)
                         (some-> callback submit)
                         (recur)))))))
         (catch IOException ex
           (handle-close key submit ex opts)))))

(defn- handle-key [^SelectionKey key submit pending opts]
  (when (and (.isValid key) (.isAcceptable key))
    (handle-accept key submit pending opts))
  (when (and (.isValid key) (.isReadable key))
    (handle-read key submit opts))
  (when (and (.isValid key) (.isWritable key))
    (handle-write key submit opts))
  true)

(defn- has-read-data? [^SelectionKey key]
  (pos? (.position ^ByteBuffer (:read-buffer (.attachment key)))))

(defn- handle-control [^SelectionKey key submit opts]
  (when-not (has-flag? key WORKING)
    (let [{:keys [^Queue control-queue]} (.attachment key)]
      (loop [resumed? false]
        (if-some [[event callback] (.poll control-queue)]
          (case event
            ::pause-reads  (do (set-flag key PAUSED)
                               (some-> callback submit)
                               (recur resumed?))
            ::resume-reads (do (unset-flag key PAUSED)
                               (some-> callback submit)
                               (recur true)))
          (when (and resumed? (has-read-data? key))
            (submit-read-handler key submit opts))))
      true)))

(defn- server-loop
  [^ServerSocketChannel server-ch ^Selector selector
   ^ExecutorService executor opts]
  (let [submit  #(.submit executor ^Runnable %)
        pending (ConcurrentHashMap/newKeySet)]
    (try
      (loop []
        (when (.isOpen server-ch)
          (.select selector)
          (foreach! #(handle-key % submit pending opts)
                    (.selectedKeys selector))
          (foreach! #(handle-control % submit opts) pending)
          (recur)))
      (finally
        (run! #(handle-close % submit nil opts) (.keys selector))
        (.close selector)
        (.shutdown executor)))))

(defn- new-default-executor []
  (let [processors (.availableProcessors (Runtime/getRuntime))]
    (Executors/newFixedThreadPool (+ 2 processors))))

(defprotocol Server
  (server-channel ^java.nio.channels.ServerSocketChannel [_]
    "Return the ServerSocketChannel for the Server."))

(defn start-server
  "Start a TCP server with the supplied map of options:

  - `:port` - the port number to listen on (mandatory)
  - `:handler` - a handler function (mandatory, see below)
  - `:control-queue-size` - the max number of queued control events (default 32)
  - `:executor` - a custom ExecutorService to supply worker threads
  - `:read-buffer-size` - the read buffer size in bytes (default 8K)
  - `:recv-buffer-size` - the receive buffer size (i.e. the SO_RCVBUF option)
  - `:reuse-address?` - sets the SO_REUSEADDR socket option (default false)
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
  ^Closeable [{:keys [port executor recv-buffer-size reuse-address?] :as opts}]
  {:pre [(int? port)]}
  (let [server-ch (server-socket-channel port)
        selector  (server-selector server-ch)
        executor  (or executor (new-default-executor))]
    (when reuse-address?
      (.setOption server-ch StandardSocketOptions/SO_REUSEADDR true))
    (when recv-buffer-size
      (.setOption server-ch StandardSocketOptions/SO_RCVBUF
                  (Integer. ^long recv-buffer-size)))
    (.start (Thread. #(server-loop server-ch selector executor opts)))
    (reify Server
      (server-channel [_] server-ch)
      Closeable
      (close [_]
        (.close server-ch)
        (.wakeup selector)))))
