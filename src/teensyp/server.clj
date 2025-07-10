(ns teensyp.server
  (:import [java.io IOException]
           [java.net InetSocketAddress]
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

(defn- update-ops [^SelectionKey key f op]
  (.interestOps key (f (.interestOps key) op)))

(def closed (Object.))

(defn- write-buffer [^SelectionKey key buffer]
  (-> key .attachment :write-queue (.add buffer))
  (update-ops key bit-or SelectionKey/OP_WRITE))

(defn- new-context [{:keys [init buffer-size]
                     :or   {buffer-size 8192}}]
  {:write-queue (ArrayDeque.)
   :read-state  (volatile! init)
   :read-buffer (ByteBuffer/allocate buffer-size)})

(defn- handle-accept
  [^SelectionKey key submit {:keys [write handler] :as opts}]
  (let [^Selector selector (-> key .selector)
        ^SocketChannel  ch (-> key .channel .accept)]
    (.configureBlocking ch false)
    (let [context (new-context opts)
          key     (.register ch selector 0 context)
          writef  #(write-buffer key (some-> % write))]
      (submit
       #(try (vswap! (:read-state context) handler writef)
             (finally
               (update-ops key bit-or SelectionKey/OP_READ)
               (.wakeup selector)))))))

(defn- handle-close
  [^SelectionKey key submit ex {:keys [close]}]
  (let [read-state (-> key .attachment :read-state)]
    (-> key .channel .close)
    (submit #(vswap! read-state close ex))))

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
  [^SelectionKey key submit {:keys [handler read write] :as opts}]
  (let [{:keys [^ByteBuffer read-buffer read-state]} (.attachment key)
        ^SocketChannel  ch (-> key .channel)
        ^Selector selector (-> key .selector)
        writef #(write-buffer key (some-> % write))]
    (update-ops key bit-and-not SelectionKey/OP_READ)
    (try
      (if (neg? (.read ch read-buffer))
        (handle-close key submit nil opts)
        (do (.flip read-buffer)
            (submit
             #(try (vswap! read-state read read-buffer)
                   (vswap! read-state handler writef)
                   (finally
                     (.compact read-buffer)
                     (update-ops key bit-or SelectionKey/OP_READ)
                     (.wakeup selector))))))
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
