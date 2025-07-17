(ns teensyp.server
  (:import [java.io IOException]
           [java.net InetSocketAddress]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey
            ServerSocketChannel SocketChannel]
           [java.util Queue]
           [java.util.concurrent ArrayBlockingQueue Executors ExecutorService]
           [java.util.concurrent.atomic AtomicBoolean AtomicInteger]))

(def CLOSE
  "A unique identifier that can be passed to the write function of a handler
  in order to close the connection."
  (Object.))

(def PAUSE-READS
  "A unique identifier that can be passed to the write function of a handler
  in order to pause reads. See: RESUME-READS."
  (Object.))

(def RESUME-READS
  "A unique identifier that can be passed to the write function of a handler
  in order to pause reads. See: RESUME-READS."
  (Object.))

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
    (locking key
      (let [flags (vswap! vflags f)]
        (when (.isValid key)
          (.interestOps key (interest-ops flags)))
        flags))))

(defn- set-flag [key flag]
  (update-flags key #(bit-or % flag)))

(defn- unset-flag [key flag]
  (update-flags key #(bit-and-not % flag)))

(defn- has-flag? [key flag]
  (-> key .attachment :flags deref (bit-flag-set? flag)))

(defn- ex-write-queue-full []
  (ex-info "Write queue full" {:err ::write-queue-full}))

(defn- ex-write-queue-over-capacity []
  (ex-info "Write queue over capacity" {:err ::write-queue-over-capacity}))

(defn- update-write-limit
  [^AtomicInteger limit ^ByteBuffer buffer]
  (let [remaining (.remaining buffer)]
    (when (> remaining (.getAndAdd limit (- remaining)))
      (.getAndAdd limit remaining)
      (throw (ex-write-queue-over-capacity)))))

(defn- writer [^SelectionKey key]
  (fn write
    ([buffer] (write buffer nil))
    ([buffer callback]
     (let [{:keys [^ArrayBlockingQueue write-queue write-limit]}
           (.attachment key)]
       (when (zero? (.remainingCapacity write-queue))
         (throw (ex-write-queue-full)))
       (when (instance? ByteBuffer buffer)
         (update-write-limit write-limit buffer))
       (.add write-queue [buffer callback])
       (set-flag key writing)
       (-> key .selector .wakeup)))))

(defn- new-context
  [{:keys [read-buffer-size write-buffer-size write-queue-size]
    :or   {read-buffer-size  8192
           write-buffer-size 32768
           write-queue-size  64}}]
  {:write-queue (ArrayBlockingQueue. write-queue-size)
   :write-limit (AtomicInteger. write-buffer-size)
   :flags       (volatile! 0)
   :state       (volatile! nil)
   :read-buffer (ByteBuffer/allocate read-buffer-size)
   :closef      (volatile! nil)})

(defn- close-key [^SelectionKey key submit ex handler]
  (let [state (-> key .attachment :state)]
    (submit #(vswap! state handler ex))))

(defn- handle-close [^SelectionKey key submit ex {:keys [handler]}]
  (let [closef (-> key .attachment :closef)]
    (-> key .channel .close)
    (set-flag key closed)
    (locking key
      (if (has-flag? key working)
        (vreset! closef #(close-key key submit ex handler))
        (close-key key submit ex handler)))))

(defn- handle-accept
  [^SelectionKey key submit {:keys [handler] :as opts}]
  (let [^Selector selector (-> key .selector)
        ^SocketChannel  ch (-> key .channel .accept)]
    (.configureBlocking ch false)
    (let [{:keys [state] :as context} (new-context opts)
          key (.register ch selector 0 context)]
      (set-flag key working)
      (submit #(try (vreset! state (handler (writer key)))
                    (finally
                      (let [flags (unset-flag key working)]
                        (if (bit-flag-set? flags closed)
                          (@(-> key .attachment :closef))
                          (.wakeup selector)))))))))

(defn- handle-write [^SelectionKey key submit opts]
  (let [{:keys [^Queue         write-queue
                ^AtomicInteger write-limit]} (.attachment key)
        ^SocketChannel ch (-> key .channel)]
    (try (loop []
           (if-some [[buffer callback] (.peek write-queue)]
             (condp identical? buffer
               CLOSE        (.close ch)
               PAUSE-READS  (do (set-flag key paused)
                                (.poll write-queue)
                                (some-> callback submit)
                                (recur))
               RESUME-READS (do (unset-flag key paused)
                                (.poll write-queue)
                                (some-> callback submit)
                                (recur))
               (do (.getAndAdd write-limit (.write ch ^ByteBuffer buffer))
                   (when-not (.hasRemaining ^ByteBuffer buffer)
                     (.poll write-queue)
                     (some-> callback submit)
                     (recur))))
             (unset-flag key writing)))
         (catch IOException ex
           (handle-close key submit ex opts)))))

(defn- handle-read
  [^SelectionKey key submit {:keys [handler] :as opts}]
  (let [{:keys [^ByteBuffer read-buffer state]} (.attachment key)
        ^SocketChannel  ch (-> key .channel)
        ^Selector selector (-> key .selector)]
    (try
      (if (neg? (.read ch read-buffer))
        (handle-close key submit nil opts)
        (do (.flip read-buffer)
            (set-flag key working)
            (submit #(try (vswap! state handler read-buffer (writer key))
                          (finally
                            (.compact read-buffer)
                            (let [flags (unset-flag key working)]
                              (if (bit-flag-set? flags closed)
                                (@(-> key .attachment :closef))
                                (.wakeup selector))))))))
      (catch IOException ex
        (handle-close key submit ex opts)))))

(defn- handle-key [^SelectionKey key submit opts]
  (when (.isValid key)
    (cond
      (.isAcceptable key) (handle-accept key submit opts)
      (.isWritable key)   (handle-write key submit opts)
      (.isReadable key)   (handle-read key submit opts))))

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

  The handler function must have three arities:  - `:read-buffer-size` - the buffer size in bytes (default 8K)


      (fn handler
        ([write] initial-state)           ;; on socket accept
        ([state buffer write] new-state)  ;; on socket read data
        ([state exception]))              ;; on socket close

  The `buffer` is a java.nio.ByteBuffer instance, and `write` is a function
  that takes a buffer as an argument and will queue it to send to the client.
  To close the channel, pass `teensyp.server/CLOSE` to the write function.

  The write function may also take `teensyp.server/PAUSE-READS` and
  `teensyp.server/RESUME-READS`. These will pause and resume reads calls
  respectively.

  You may optionally specify a second argument to `write`. This is should be
  a zero-argument callback function, which is called after the write completes.

  The `state` is a custom data structure that is returned when the accept or
  read arities are triggered. A different state is associated with each
  connection.

  When closing, the `exception` may contain the exception that terminated the
  channel, or `nil` if the channel were terminated gracefully.

  The handler function is guaranteed to execute in serial per channel. That is,
  the accept will always be first, the close will always be last, and reads
  will always be sequential."
  [{:keys [port executor] :as opts}]
  {:pre [(int? port)]}
  (let [server-ch (server-socket-channel port)
        selector  (server-selector server-ch)
        executor  (or executor (new-default-executor))]
    (.start (Thread. #(server-loop server-ch selector executor opts)))
    server-ch))
