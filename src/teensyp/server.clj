(ns teensyp.server
  (:import [java.net InetSocketAddress]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey
            ServerSocketChannel SocketChannel]
           [java.util ArrayDeque]
           [java.util.concurrent Executors ExecutorService]))

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

(defn- update-interest [^SelectionKey key f op]
  (.interestOps key (f (.interestOps key) op)))

(defn write [^SelectionKey key buffer]
  (-> key .attachment :write-queue (.add buffer))
  (update-interest key bit-or SelectionKey/OP_WRITE))

(defn close [key]
  (write key ::close))

(defn- handle-accept [^Selector selector ^SelectionKey key]
  (let [^SocketChannel ch (-> key .channel .accept)]
    (.configureBlocking ch false)
    (let [key (.register ch selector SelectionKey/OP_READ
                         {:write-queue (ArrayDeque.)})]
      (write key (ByteBuffer/wrap (.getBytes "hello\n")))
      (close key))))

(defn- handle-write [^SelectionKey key]
  (let [^ArrayDeque queue (-> key .attachment :write-queue)
        ^SocketChannel ch (-> key .channel)]
    (loop []
      (if-some [buffer (.peek queue)]
        (if (= ::close buffer)
          (.close ch)
          (do (.write ch ^ByteBuffer buffer)
              (when-not (.hasRemaining ^ByteBuffer buffer)
                (.poll queue)
                (recur))))
        (update-interest key bit-and-not SelectionKey/OP_WRITE)))))

(defn- handle-key
  [selector ^SelectionKey key ^ExecutorService executor]
  (letfn [(submit [f] (.submit executor ^Runnable f))]
    (when (.isValid key)
      (cond
        (.isAcceptable key)
        (handle-accept selector key)
        (.isWritable key)
        (handle-write key)))))

(defn- server-loop
  [^ServerSocketChannel server-ch ^Selector selector executor]
  (loop []
    (when (.isOpen server-ch)
      (.select selector)
      (foreach! #(handle-key selector % executor) (.selectedKeys selector))
      (recur))))

(defn- start-daemon-thread [^Runnable r]
  (doto (Thread. r) (.setDaemon true) (.start)))

(defn- new-default-executor []
  (let [processors (.availableProcessors (Runtime/getRuntime))]
    (Executors/newFixedThreadPool (+ 2 processors))))

(defn start-server
  [{:keys [port executor]}]
  {:pre [(int? port)]}
  (let [server-ch (server-socket-channel port)
        selector  (server-selector server-ch)
        executor  (or executor (new-default-executor))]
    (start-daemon-thread #(server-loop server-ch selector executor))
    server-ch))
