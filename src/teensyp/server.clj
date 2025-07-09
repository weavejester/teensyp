(ns teensyp.server
  (:import [java.net InetSocketAddress]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey
            ServerSocketChannel SocketChannel]
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

(defn- handle-accept [^SelectionKey key]
  (let [^SocketChannel ch (-> key .channel .accept)]
    (.configureBlocking ch false)
    (.write ch (ByteBuffer/wrap (.getBytes "hello\n")))
    (.close ch)))

(defn- handle-key [^SelectionKey key ^ExecutorService executor]
  (when (.isValid key)
    (when (.isAcceptable key)
      (.submit executor ^Runnable #(handle-accept key)))))

(defn- server-loop
  [^ServerSocketChannel server-ch ^Selector selector executor]
  (loop []
    (when (.isOpen server-ch)
      (.select selector)
      (foreach! #(handle-key % executor) (.selectedKeys selector))
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
