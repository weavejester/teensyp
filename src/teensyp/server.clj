(ns teensyp.server
  (:import [java.net InetSocketAddress]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey
            ServerSocketChannel SocketChannel]))

(defn- server-socket-channel ^ServerSocketChannel [port]
  (doto (ServerSocketChannel/open)
    (.configureBlocking false)
    (.bind (InetSocketAddress. port))))

(defn- server-selector [^ServerSocketChannel server-ch]
  (let [selector (Selector/open)]
    (.register server-ch selector SelectionKey/OP_ACCEPT)
    selector))

(defn- iter-selection-keys [^Selector selector f]
  (let [iter (-> selector .selectedKeys .iterator)]
    (loop []
      (when (.hasNext iter)
        (f (.next iter))
        (.remove iter)
        (recur)))))

(defn- handle-key [^SelectionKey key]
  (when (.isValid key)
    (when (.isAcceptable key)
      (let [^SocketChannel ch (-> key .channel .accept)]
        (.configureBlocking ch false)
        (.write ch (ByteBuffer/wrap (.getBytes "hello\n")))
        (.close ch)))))

(defn- server-loop [^ServerSocketChannel server-ch ^Selector selector]
  (loop []
    (when (.isOpen server-ch)
      (.select selector)
      (iter-selection-keys selector handle-key)
      (recur))))

(defn- start-daemon-thread [^Runnable r]
  (doto (Thread. r) (.setDaemon true) (.start)))

(defn start-server [{:keys [port]}]
  {:pre [(int? port)]}
  (let [server-ch (server-socket-channel port)
        selector  (server-selector server-ch)]
    (start-daemon-thread #(server-loop server-ch selector))
    server-ch))
