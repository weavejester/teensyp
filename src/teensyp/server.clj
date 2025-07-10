(ns teensyp.server
  (:import [java.io IOException]
           [java.net InetSocketAddress]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey
            ServerSocketChannel SocketChannel]
           [java.util ArrayDeque]
           [java.util.concurrent Executors ExecutorService]))

(def closed
  "A unique object that can be passed to the write function of a handler
  in order to close the connection."
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

(defn- write [^SelectionKey key buffer]
  (-> key .attachment :write-queue (.add buffer))
  (update-ops key bit-or SelectionKey/OP_WRITE))

(defn- new-context [{:keys [buffer-size] :or {buffer-size 8192}}]
  {:write-queue (ArrayDeque.)
   :state       (volatile! nil)
   :read-buffer (ByteBuffer/allocate buffer-size)})

(defn- handle-accept
  [^SelectionKey key submit {:keys [handler] :as opts}]
  (let [^Selector selector (-> key .selector)
        ^SocketChannel  ch (-> key .channel .accept)]
    (.configureBlocking ch false)
    (let [{:keys [state] :as context} (new-context opts)
          key (.register ch selector 0 context)]
      (submit (fn []
                (try (vreset! state (handler #(write key %)))
                     (finally
                       (update-ops key bit-or SelectionKey/OP_READ)
                       (.wakeup selector))))))))

(defn- handle-close
  [^SelectionKey key submit ex {:keys [handler]}]
  (let [state (-> key .attachment :state)]
    (-> key .channel .close)
    (submit #(vswap! state handler ex))))

(defn- handle-write [^SelectionKey key submit opts]
  (let [^ArrayDeque queue (-> key .attachment :write-queue)
        ^SocketChannel ch (-> key .channel)]
    (try (loop []
           (if-some [buffer (.peek queue)]
             (if (identical? buffer closed)
               (.close ch)
               (do (.write ch ^ByteBuffer buffer)
                   (when-not (.hasRemaining ^ByteBuffer buffer)
                     (.poll queue)
                     (recur))))
             (update-ops key bit-and-not SelectionKey/OP_WRITE)))
         (catch IOException ex
           (handle-close key submit ex opts)))))

(defn- handle-read
  [^SelectionKey key submit {:keys [handler] :as opts}]
  (let [{:keys [^ByteBuffer read-buffer state]} (.attachment key)
        ^SocketChannel  ch (-> key .channel)
        ^Selector selector (-> key .selector)]
    (update-ops key bit-and-not SelectionKey/OP_READ)
    (try
      (if (neg? (.read ch read-buffer))
        (handle-close key submit nil opts)
        (do (.flip read-buffer)
            (submit
             (fn []
               (try (vswap! state handler read-buffer #(write key %))
                    (finally
                      (.compact read-buffer)
                      (update-ops key bit-or SelectionKey/OP_READ)
                      (.wakeup selector)))))))
      (catch IOException ex
        (handle-close key submit ex opts)))))

(defn- handle-key [^SelectionKey key submit opts]
  (when (.isValid key)
    (cond
      (.isAcceptable key) (handle-accept key submit opts)
      (.isReadable key)   (handle-read key submit opts)
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
  To close the channel, pass `teensyp.server/closed` to the write function.

  The `state` is a custom data structure that is returned when the accept or
  read arities are triggered. A different state is associated with each
  connection.

  When closing, the `exception` may contain the exception that terminated the
  channel, or `nil` if the channel were terminated gracefully."
  [{:keys [port executor] :as opts}]
  {:pre [(int? port)]}
  (let [server-ch (server-socket-channel port)
        selector  (server-selector server-ch)
        executor  (or executor (new-default-executor))]
    (.start (Thread. #(server-loop server-ch selector executor opts)))
    server-ch))
