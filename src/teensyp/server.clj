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

(def ^:private closed (Object.))

(defn- write-buffer [^SelectionKey key buffer]
  (-> key .attachment :write-queue (.add (or buffer closed)))
  (update-interest key bit-or SelectionKey/OP_WRITE))

(defn- handle-accept
  [^Selector selector ^SelectionKey key submit {:keys [init write handler]}]
  (let [^SocketChannel ch (-> key .channel .accept)]
    (.configureBlocking ch false)
    (let [data   (volatile! init)
          key    (.register ch selector 0 {:write-queue (ArrayDeque.)
                                           :read-data   data})
          writef #(write-buffer key (some-> % write))]
      (submit
       #(try (vswap! data handler writef)
             (finally
               (update-interest key bit-or SelectionKey/OP_READ)
               (.wakeup selector)))))))

(defn- handle-write [^SelectionKey key _opts]
  (let [^ArrayDeque queue (-> key .attachment :write-queue)
        ^SocketChannel ch (-> key .channel)]
    (loop []
      (if-some [buffer (.peek queue)]
        (if (identical? buffer closed)
          (.close ch)
          (do (.write ch ^ByteBuffer buffer)
              (when-not (.hasRemaining ^ByteBuffer buffer)
                (.poll queue)
                (recur))))
        (update-interest key bit-and-not SelectionKey/OP_WRITE)))))

(defn- handle-key [selector ^SelectionKey key submit opts]
  (when (.isValid key)
    (cond
      (.isAcceptable key)
      (handle-accept selector key submit opts)
      (.isWritable key)
      (handle-write key opts))))

(defn- server-loop
  [^ServerSocketChannel server-ch ^Selector selector executor opts]
  (letfn [(submit [f] (.submit executor ^Runnable f))]
    (try
      (loop []
        (when (.isOpen server-ch)
          (.select selector)
          (foreach! #(handle-key selector % submit opts)
                    (.selectedKeys selector))
          (recur)))
      (finally
        (.shutdown executor)))))

(defn- start-daemon-thread [^Runnable r]
  (doto (Thread. r) (.setDaemon true) (.start)))

(defn- new-default-executor []
  (let [processors (.availableProcessors (Runtime/getRuntime))]
    (Executors/newFixedThreadPool (+ 2 processors))))

(defn start-server
  [{:keys [port executor] :as opts}]
  {:pre [(int? port)]}
  (let [server-ch (server-socket-channel port)
        selector  (server-selector server-ch)
        executor  (or executor (new-default-executor))]
    (start-daemon-thread #(server-loop server-ch selector executor opts))
    server-ch))
