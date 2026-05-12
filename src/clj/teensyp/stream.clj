(ns teensyp.stream
  (:require [teensyp.server :as t])
  (:import [java.io IOException]
           [java.nio ByteBuffer]
           [java.util.concurrent Executors ExecutorService]
           [java.util.concurrent.locks Condition Lock ReentrantLock]
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

(defmacro ^:private with-lock [lock & body]
  `(let [^Lock lock# ~lock]
     (.lock lock#)
     (try ~@body (finally (.unlock lock#)))))

(defn- new-default-executor []
  (Executors/newFixedThreadPool 32))

(defn stream-handler
  ([handler]
   (stream-handler handler {}))
  ([handler {:keys [executor read-buffer-size] :or {read-buffer-size 8192}}]
   (fn
     ([write]
      (let [executor   (or executor (new-default-executor))
            write-lock (ReentrantLock.)
            read-lock  (ReentrantLock.)
            can-read   (.newCondition read-lock)
            buffer     (.flip (ByteBuffer/allocate read-buffer-size))
            closed?    (volatile! false)
            readf      (fn [b off len]
                         (with-lock read-lock
                           (loop []
                             (cond
                              @closed?   -1
                              (zero? len) 0
                              (not (.hasRemaining buffer))
                              (do (.await can-read) (recur))
                              :else
                              (let [len (min len (.remaining buffer))]
                                (.get buffer b off len)
                                (when (.hasRemaining buffer) (.signal can-read))
                                len)))))
            writef     (fn [b off len]
                         (with-lock write-lock
                           (if @closed?
                             (throw (IOException. "Closed"))
                             (write (ByteBuffer/wrap b off len)))))
            closef     (fn []
                         (with-lock write-lock
                           (when-not @closed?
                             (write t/CLOSE)
                             (vreset! closed? true))))
            input      (input-stream readf closef)
            output     (output-stream writef closef)]
        (.submit ^ExecutorService executor ^Runnable #(handler input output))
        {:buffer     buffer
         :can-read   can-read 
         :closed?    closed?
         :read-lock  read-lock
         :write-lock write-lock}))
     ([{:keys [buffer can-read read-lock] :as state} buf _write]
      (with-lock read-lock
        (.compact buffer)
        (.put buffer buf)
        (.flip buffer)
        (.signal ^Condition can-read)
        state))
     ([{:keys [read-lock write-lock can-read closed?]} _ex]
      (with-lock read-lock
        (with-lock write-lock
          (vreset! closed? true)
          (.signal ^Condition can-read)))))))
     
        
                          
                         
                       
                      
                         
                         
       
  
