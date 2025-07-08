(ns teensyp.server
  (:import [java.net InetSocketAddress]
           [java.nio.channels ServerSocketChannel]))

(defn- server-socket-channel ^ServerSocketChannel [port]
  (doto (ServerSocketChannel/open)
    (.configureBlocking false)
    (.bind (InetSocketAddress. port))))

(defn- start-daemon-thread [^Runnable r]
  (doto (Thread. r) (.setDaemon true) (.start)))

(defn- server-loop [^ServerSocketChannel server-ch]
  (loop []
    (when (.isOpen server-ch)
      (Thread/sleep 1000)
      (recur))))

(defn start-server [{:keys [port]}]
  {:pre [(int? port)]}
  (let [server-ch (server-socket-channel port)]
    (start-daemon-thread #(server-loop server-ch))
    (fn stop-server [] (.close server-ch))))
