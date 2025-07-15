(ns teensyp.server
  (:import [java.io IOException]
           [java.net InetSocketAddress]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey
            ServerSocketChannel SocketChannel]
           [java.util ArrayDeque]
           [java.util.concurrent Executors ExecutorService]))

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

(defn- update-ops [^SelectionKey key f op]
  (.interestOps key (f (.interestOps key) op)))

(defn- writer [^SelectionKey key]
  (fn write
    ([buffer] (write buffer nil))
    ([buffer callback]
     (-> key .attachment :write-queue (.add [buffer callback]))
     (update-ops key bit-or SelectionKey/OP_WRITE)
     (-> key .selector .wakeup))))

(defn- new-context [{:keys [buffer-size] :or {buffer-size 8192}}]
  {:write-queue (ArrayDeque.)
   :state       (volatile! nil)
   :read-buffer (ByteBuffer/allocate buffer-size)
   :working?    (atom false)
   :closef      (volatile! nil)
   :paused?     (volatile! false)})

(defn- close-key [^SelectionKey key submit ex handler]
  (let [state (-> key .attachment :state)]
    (submit #(vswap! state handler ex))))

(defn- handle-close [^SelectionKey key submit ex {:keys [handler]}]
  (-> key .channel .close)
  (let [{:keys [working? closef]} (.attachment key)]
    (vreset! closef #(close-key key submit ex handler))
    (when-not (compare-and-set! working? true false)
      (close-key key submit ex handler))))

(defn- handle-accept
  [^SelectionKey key submit {:keys [handler] :as opts}]
  (let [^Selector selector (-> key .selector)
        ^SocketChannel  ch (-> key .channel .accept)]
    (.configureBlocking ch false)
    (let [{:keys [state working?] :as context} (new-context opts)
          key (.register ch selector 0 context)]
      (reset! working? true)
      (submit #(try (vreset! state (handler (writer key)))
                    (finally
                      (when-not (compare-and-set! working? true false)
                        (@(-> key .attachment :closef)))
                      (update-ops key bit-or SelectionKey/OP_READ)
                      (.wakeup selector)))))))

(defn- pause-key [^SelectionKey key]
  (vreset! (-> key .attachment :paused?) true)
  (update-ops key bit-and-not SelectionKey/OP_READ))

(defn- resume-key [^SelectionKey key]
  (vreset! (-> key .attachment :paused?) false)
  (update-ops key bit-or SelectionKey/OP_READ))

(defn- handle-write [^SelectionKey key submit opts]
  (let [^ArrayDeque queue (-> key .attachment :write-queue)
        ^SocketChannel ch (-> key .channel)]
    (try (loop []
           (if-some [[buffer callback] (.peek queue)]
             (condp identical? buffer
               CLOSE        (.close ch)
               PAUSE-READS  (do (pause-key key)
                                (.poll queue)
                                (some-> callback submit)
                                (recur))
               RESUME-READS (do (resume-key key)
                                (.poll queue)
                                (some-> callback submit)
                                (recur))
               (do (.write ch ^ByteBuffer buffer)
                   (when-not (.hasRemaining ^ByteBuffer buffer)
                     (.poll queue)
                     (some-> callback submit)
                     (recur))))
             (update-ops key bit-and-not SelectionKey/OP_WRITE)))
         (catch IOException ex
           (handle-close key submit ex opts)))))

(defn- handle-read
  [^SelectionKey key submit {:keys [handler] :as opts}]
  (let [{:keys [^ByteBuffer read-buffer state working?]} (.attachment key)
        ^SocketChannel  ch (-> key .channel)
        ^Selector selector (-> key .selector)]
    (update-ops key bit-and-not SelectionKey/OP_READ)
    (try
      (if (neg? (.read ch read-buffer))
        (handle-close key submit nil opts)
        (do (.flip read-buffer)
            (reset! working? true)
            (submit #(try (vswap! state handler read-buffer (writer key))
                          (finally
                            (when-not (compare-and-set! working? true false)
                              (@(-> key .attachment :closef)))
                            (.compact read-buffer)
                            (update-ops key bit-or SelectionKey/OP_READ)
                            (.wakeup selector))))))
      (catch IOException ex
        (handle-close key submit ex opts)))))

(defn- handle-key [^SelectionKey key submit opts]
  (when (.isValid key)
    (cond
      (.isAcceptable key) (handle-accept key submit opts)
      (.isWritable key)   (handle-write key submit opts)
      (.isReadable key)   (if (-> key .attachment :paused? deref)
                            (update-ops key bit-and-not SelectionKey/OP_READ)
                            (handle-read key submit opts)))))

;; The handler is called when accepting, reading and closing a socket channel,
;; and is always run within a worker thread assigned by the executor.
;;
;; Before an accept or read, the selector is told to ignore future reads for
;; the associated selection key. When the handler has completed, reads are
;; turned back on for the key. This ensures that the selector cannot start a
;; new read for the key until the existing one completes.
;;
;; Also before an accept or read, the :working? atom is set to true. When the
;; handler completes, it is set to false. If the channel needs to close
;; suddenly, the :working? atom is checked. If it's true, then the handler is
;; working; the close handler is queued and :working? is set to false. This
;; indicates to the worker thread that the close handler needs to be run after
;; it. Otherwise, the close handler is run immediately.
;;
;; The close event is always triggered from the main server thread, so worker
;; threads running the handler are our only concern. We use compare-and-set!
;; to atomically decide whether to run the close handler immediately or queue
;; it to be run after the worker thread. The queued close handler uses a
;; volatile because we know it will only be read after the :working? atom
;; is set, so no two threads have simultaneous access.

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
  - `:buffer-size` - the buffer size in bytes (default 8K)

  The handler function must have three arities:

      (fn handler
        ([write] initial-state)           ;; on socket accept
        ([state buffer write] new-state)  ;; on socket read data
        ([state exception]))              ;; on socket close

  The `buffer` is a java.nio.ByteBuffer instance, and `write` is a function
  that takes a buffer as an argument and will queue it to send to the client.
  To close the channel, pass `teensyp.server/CLOSE` to the write function.

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
