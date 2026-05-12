(ns teensyp.stream
  (:require [teensyp.server :as t])
  (:import [java.io IOException]
           [java.nio ByteBuffer]
           [java.util.concurrent ExecutorService]
           [java.util.concurrent.locks Condition ReentrantLock]
           [teensyp IInputStream IOutputStream
                   ProxyInputStream ProxyOutputStream]))

(defn input-stream
  ([readf]
   (input-stream readf (fn [])))
  ([readf closef]
   (ProxyInputStream.
    (reify IInputStream
      (read [_ b off len] (readf b off len))
      (close [_] (closef))))))

(defn output-stream
  ([writef]
   (output-stream writef (fn [])))
  ([writef closef]
   (output-stream writef closef (fn [])))
  ([writef closef flushf]
   (ProxyOutputStream.
    (reify IOutputStream
      (write [_ b off len] (writef b off len))
      (close [_] (closef))
      (flush [_] (flushf))))))

(defn stream-handler
  [^ExecutorService executor handler
   {:keys [read-buffer-size] :or {read-buffer-size 8192}}]
  (fn
    ([write]
     (let [write-lock (ReentrantLock.)
           read-lock  (ReentrantLock.)
           can-read   (.newCondition read-lock)
           buffer     (doto (ByteBuffer/allocate read-buffer-size) .flip)
           closed?    (volatile! false)
           closef     #(write t/CLOSE) 
           input
           (input-stream
            (fn [b off len]
              (.lock read-lock)
              (try
                (loop []
                  (cond
                    @closed?   -1
                    (zero? len) 0
                    (not (.hasRemaining buffer)) (do (.await can-read) (recur))
                    :else
                    (let [len (min len (.remaining buffer))]
                      (.get buffer b off len)
                      (when (.hasRemaining buffer) (.signal can-read))
                      len)))
                (finally (.unlock read-lock))))
            closef)
           output
           (output-stream
            (fn [b off len]
              (try
                (.lock write-lock)
                (if @closed?
                  (throw (IOException. "Closed"))
                  (do (write (ByteBuffer/wrap b off len)) len))
                (finally (.unlock write-lock))))
            closef)]
       (.submit executor ^Runnable #(handler input output))
       {:read-lock  read-lock
        :write-lock write-lock
        :can-read   can-read 
        :buffer     buffer
        :closed?    closed?}))
    ([{:keys [buffer can-read read-lock] :as state} buf _write]
     (.lock ^ReentrantLock read-lock)
     (try
       (.compact buffer)
       (.put buffer buf)
       (.flip buffer)
       (.signal ^Condition can-read)
       state
       (finally (.unlock ^ReentrantLock read-lock))))
    ([{:keys [read-lock write-lock can-read closed?]} _ex]
     (.lock ^ReentrantLock read-lock)
     (.lock ^ReentrantLock write-lock)
     (try
       (vreset! closed? true)
       (.signal ^Condition can-read)
       (finally
         (.unlock ^ReentrantLock read-lock)
         (.unlock ^ReentrantLock write-lock))))))
     
        
                          
                         
                       
                      
                         
                         
       
  
