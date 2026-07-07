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
            Executors ExecutorService TimeUnit]
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

(def ^:private ^:const WRITING 0x01)
(def ^:private ^:const WORKING 0x02)
(def ^:private ^:const CLOSED  0x04)
(def ^:private ^:const PAUSED  0x08)
(def ^:private ^:const FULL    0x10)

(defn- bit-flag-set? [flags flag]
  (not (zero? (bit-and flags flag))))

(def ^:private ^:const READ_MASK  (bit-or PAUSED CLOSED FULL))
(def ^:private ^:const WRITE_MASK (bit-or WRITING CLOSED))

(defn- interest-ops [flags]
  (bit-or (if (bit-flag-set? flags READ_MASK)
            0 SelectionKey/OP_READ)
          (if (= WRITING (bit-and flags WRITE_MASK))
            SelectionKey/OP_WRITE 0)))

(defn- update-flags [^SelectionKey key f]
  (let [context (.attachment key)
        vflags  (:flags context)]
    (with-lock (:lock context)
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
  (try-write [socket buffer]
    "Try to immediately write as much of a buffer as it can to the socket
    without blocking. Returns true if the write completed in its entirity,
    false if it failed. See [[write]] for a more convenient function.")
  (queue-control [socket control callback]
    "Queue a control event, such as ::pause-reads or ::resume-reads.")
  (queue-write [socket buffer callback]
    "Queue the buffer to be written to the socket. If the callback is not nil,
    it will be called as a zero argument function once the buffer has been
    written. See [[write]] for a more convenient function.")
  (socket-info [socket]
    "Return a map of information about the socket's connection. This includes
    keys for :local-address and :remote-address, which are immutable
    java.net.InetSocketAddress instances.")
  (socket-lock [socket]
    "Return the ReentrantLock for the socket. Useful for guaranteeing
    multiple atomic writes."))

(defn write
  "Write a ByteBuffer to a socket defined by the Socket protocol. Accepts an
  optional, zero argument callback function that will be run after the buffer
  has been written.
  
  The write will be attempted immediately; if the buffer could not be written
  in its entirity, the remaining bytes are queued to be written later. The
  callback function will only be called when all bytes are written to the
  socket.
  
  The maximum number of queued buffers per socket is governed by the
  `:write-queue-size` option on the server, and the maximum size of bytes
  that can be held in *all* queued buffers is `:write-buffer-size`. Exceeding
  either of these limits will throw an ExceptionInfo."
  ([socket buffer] (write socket buffer nil))
  ([socket buffer callback]
   (with-lock (socket-lock socket)
     (if (try-write socket buffer)
       (when callback (callback))
       (queue-write socket buffer callback)))))

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
  (try-write [key buffer]
    (when-not (identical? buffer ::close)
      (let [^ArrayBlockingQueue queue (-> key .attachment :write-queue)]
        (when (.isEmpty queue)
          (try (.write ^SocketChannel (.channel key) ^ByteBuffer buffer)
               (not (.hasRemaining ^ByteBuffer buffer))
               (catch IOException _ex false))))))
  (queue-control [key event callback]
    (let [context                           (.attachment key)
          ^ArrayBlockingQueue control-queue (:control-queue context)
          ^Set pending-set                  (:pending-set context)]
      (when (zero? (.remainingCapacity control-queue))
        (throw (ex-control-queue-full)))
      (.add control-queue [event callback])
      (.add pending-set key)
      (-> key .selector .wakeup)))
  (queue-write [key buffer callback]
    (let [context                         (.attachment key)
          ^ArrayBlockingQueue write-queue (:write-queue context)
          write-limit                     (:write-limit context)]
      (when (zero? (.remainingCapacity write-queue))
        (throw (ex-write-queue-full)))
      (when (instance? ByteBuffer buffer)
        (update-write-limit write-limit buffer))
      (.add write-queue [buffer callback])
      (set-flag key WRITING)
      (-> key .selector .wakeup)))
  (socket-info [key]
    (-> key .attachment :socket-info))
  (socket-lock [key]
    (-> key .attachment :lock)))

(defrecord Context [close-ex control-queue flags lock pending-set read-buffer
                    read-view socket-info state write-limit write-queue])

(defn- channel-socket-info [^SocketChannel ch]
  {:local-address  (.getLocalAddress ch)
   :remote-address (.getRemoteAddress ch)})

(defn- new-context
  [^SocketChannel ch pending-set
   {:keys [control-queue-size read-buffer-size
           write-buffer-size write-queue-size]
    :or   {control-queue-size 32
           read-buffer-size   8192
           write-buffer-size  32768
           write-queue-size   64}}]
  (let [read-buffer (ByteBuffer/allocate read-buffer-size)]
    (->Context (volatile! nil)                           ; :close-ex
               (ArrayBlockingQueue. control-queue-size)  ; :control-queue
               (volatile! 0)                             ; :flags
               (ReentrantLock.)                          ; :lock
               pending-set                               ; :pending-set
               read-buffer                               ; :read-buffer
               (-> read-buffer .duplicate .flip)         ; :read-view
               (channel-socket-info ch)                  ; :socket-info
               (volatile! nil)                           ; :state
               (AtomicInteger. write-buffer-size)        ; :write-limit
               (ArrayBlockingQueue. write-queue-size)))) ; :write-queue

(defn- handle-close [^SelectionKey key ex]
  (let [context          (.attachment key)
        close-ex         (:close-ex context)
        ^Set pending-set (:pending-set context)]
    (-> key .channel .close)
    (vswap! close-ex #(or % ex))
    (set-flag key CLOSED)
    (.add pending-set key)))

(defn- handle-accept
  [^SelectionKey key submit ^Set pending-set {:keys [handler] :as opts}]
  (let [^Selector selector (.selector key)
        ^SocketChannel  ch (.accept ^ServerSocketChannel (.channel key))]
    (.configureBlocking ch false)
    (.setOption ch StandardSocketOptions/TCP_NODELAY true)
    (let [context (new-context ch pending-set opts)
          state   (:state context)
          key     (.register ch selector 0 context)]
      (set-flag key WORKING)
      (submit #(try (vreset! state (handler key))
                    (catch Exception ex
                      (handle-close key ex))
                    (finally
                      (.add pending-set key)
                      (unset-flag key WORKING)
                      (.wakeup selector)))))))

(defn- compact-buffer-by-amount [^ByteBuffer buffer ^long n]
  (if (zero? n)
    buffer
    (let [position (.position buffer)]
      (-> buffer (.position n) .compact (.position (- position n))))))

(defn- update-read-view [^ByteBuffer read-view ^ByteBuffer read-buffer]
  (.position read-view 0)
  (.limit read-view (.position read-buffer)))

(defn- submit-read-handler [^SelectionKey key submit {:keys [handler]}]
  (let [^Selector selector      (.selector key)
        context                 (.attachment key)
        ^Set pending-set        (:pending-set context)
        ^ByteBuffer read-buffer (:read-buffer context)
        ^ByteBuffer read-view   (:read-view context)
        state                   (:state context)]
    (update-read-view read-view read-buffer)
    (set-flag key WORKING)
    (submit #(try (vswap! state handler key read-view)
                  (catch Exception ex
                    (handle-close key ex))
                  (finally
                    (.add pending-set key)
                    (unset-flag key WORKING)
                    (.wakeup selector))))))

(defn- handle-read [^SelectionKey key submit opts]
  (let [context                 (.attachment key)
        ^ByteBuffer read-buffer (:read-buffer context)
        ^ByteBuffer read-view   (:read-view context)
        ^SocketChannel ch       (.channel key)
        working?                (has-flag? key WORKING)]
    (try
      (when-not working?
        (compact-buffer-by-amount read-buffer (.position read-view)))
      (when (.hasRemaining read-buffer)
        (if (neg? (.read ch read-buffer))
          (handle-close key nil)
          (do (when-not (.hasRemaining read-buffer)
                (set-flag key FULL))
              (when-not working?
                (submit-read-handler key submit opts)))))
      (catch IOException ex
        (handle-close key ex)))))

(defn- handle-write [^SelectionKey key submit]
  (let [context                    (.attachment key)
        ^Queue write-queue         (:write-queue context)
        ^AtomicInteger write-limit (:write-limit context)
        ^SocketChannel ch          (.channel key)]
    (unset-flag key WRITING)
    (try (loop []
           (when-some [[buffer callback] (.peek write-queue)]
             (if (identical? buffer ::close)
               (do (.close ch)
                   (handle-close key nil)
                   (some-> callback submit))
               (do (.getAndAdd write-limit (.write ch ^ByteBuffer buffer))
                   (if (.hasRemaining ^ByteBuffer buffer)
                     (set-flag key WRITING)    ;; partial write
                     (do (.poll write-queue)
                         (some-> callback submit)
                         (recur)))))))
         (catch IOException ex
           (handle-close key ex)))))

(defn- handle-key [^SelectionKey key submit pending opts]
  (when (and (.isValid key) (.isAcceptable key))
    (handle-accept key submit pending opts))
  (when (and (.isValid key) (.isReadable key))
    (handle-read key submit opts))
  (when (and (.isValid key) (.isWritable key))
    (handle-write key submit))
  true)

(defn- handle-pending-read [^SelectionKey key submit opts]
  (let [context                 (.attachment key)
        ^ByteBuffer read-buffer (:read-buffer context)
        ^ByteBuffer read-view   (:read-view context)]
    (when (pos? (.position read-view))
      (unset-flag key FULL))
    (when (> (.position read-buffer) (.limit read-view))
      (compact-buffer-by-amount read-buffer (.position read-view))
      (submit-read-handler key submit opts))))

(defn- handle-pending-close [^SelectionKey key submit {:keys [handler]}]
  (let [context  (.attachment key)
        close-ex (:close-ex context)
        state    (:state context)
        flags    (-> context :flags deref)]
    (when (and (bit-flag-set? flags CLOSED)
               (not (bit-flag-set? flags WORKING)))
      (submit #(handler @state @close-ex)))))

(defn- has-read-data? [^SelectionKey key]
  (let [context                 (.attachment key)
        ^ByteBuffer read-buffer (:read-buffer context)
        ^ByteBuffer read-view   (:read-view context)]
    (> (.position read-buffer) (.position read-view))))

(defn- handle-control [^SelectionKey key submit opts]
  (let [^Queue control-queue (:control-queue (.attachment key))]
    (loop [resumed? false]
      (if-some [[event callback] (.poll control-queue)]
        (case event
          ::pause-reads  (do (set-flag key PAUSED)
                             (some-> callback submit)
                             (recur resumed?))
          ::resume-reads (do (unset-flag key PAUSED)
                             (some-> callback submit)
                             (recur true)))
        (when (and resumed?
                   (not (has-flag? key WORKING))
                   (has-read-data? key))
          (submit-read-handler key submit opts))))))

(defn- handle-pending [^SelectionKey key submit opts]
  (when-not (has-flag? key WORKING)
    (handle-pending-read key submit opts)
    (handle-pending-close key submit opts)
    (handle-control key submit opts)
    true))

(defn- shutdown-key [^SelectionKey key {:keys [handler]}]
  (let [ch (.channel key)]
    (when (instance? SocketChannel ch)
      (.close ch)
      (handler (-> key .attachment :state deref) nil))))

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
          (foreach! #(handle-pending % submit opts) pending)
          (recur)))
      (finally
        (.shutdown executor)
        (.awaitTermination executor 60 TimeUnit/SECONDS)
        (run! #(shutdown-key % opts) (.keys selector))
        (.close selector)))))

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
  will always be sequential.
  
  The returned server instance implements `java.io.Closeable`, and therefore
  can be used with the `with-open` macro."
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
