(ns teensyp.stream
  "A namespace of utility functions for integrating streams into Teensyp."
  (:require [teensyp.concurrent :refer [with-lock]]
            [teensyp.server :as t])
  (:import [java.io IOException]
           [java.nio ByteBuffer]
           [java.util.concurrent Executors ExecutorService]
           [java.util.concurrent.locks Condition ReentrantLock]
           [teensyp IInputStream IOutputStream
                    ProxyInputStream ProxyOutputStream]))

(defn input-stream
  "Create an InputStream from a read and optional close function. The read
  function maps to the `.read` method on the InputStream class and takes 3
  arguments: a byte array to receive the data, an offset and a length. The read
  function should return the number of bytes read, or -1 if the stream is
  closed. The close function maps to the `.close` method and takes zero
  arguments."
  ([readf]
   (input-stream readf (fn [])))
  ([readf closef]
   (ProxyInputStream.
    (reify IInputStream
      (read [_ b off len] (if (zero? len) 0 (readf b off len)))
      (close [_] (closef))))))

(defn output-stream
  "Create an OutputStream from a write function and optional close and
  flush functions. The write function maps to the `.write` method on the
  OutputStream class and takes 3 arguments: a byte array with the data to send,
  an offset and a length. The close function maps to the `.close` method and
  takes zero arguments. Similarly the flush function maps to the `.flush`
  method and also takes zero arguments."
  ([writef]
   (output-stream writef (fn [])))
  ([writef closef]
   (output-stream writef closef (fn [])))
  ([writef closef flushf]
   (ProxyOutputStream.
    (reify IOutputStream
      (write [_ b off len] (when-not (zero? len) (writef b off len)))
      (close [_] (closef))
      (flush [_] (flushf))))))

(defn- new-default-executor []
  (Executors/newFixedThreadPool 32))

(defn stream-handler
  "Create a Teensyp server handler from a function that takes an InputStream
  and OutputStream as arguments. Accepts an options map with the following keys:

  :executor - an executor for running the handler function, defaults to a fixed
              thread pool of 32 threads
  :read-buffer-size - the size in bytes of the read buffer, defaults to 8K"
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
                              @closed? -1
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
                             (doto (ByteBuffer/allocate len)
                               (.put b off len) .flip write))))
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
     ([{:keys [buffer can-read read-lock] :as state} ^ByteBuffer buf _write]
      (with-lock read-lock
        (doto ^ByteBuffer buffer .compact (.put buf) .flip)
        (.signal ^Condition can-read)
        state))
     ([{:keys [read-lock write-lock can-read closed?]} _ex]
      (with-lock read-lock
        (with-lock write-lock
          (vreset! closed? true)
          (.signal ^Condition can-read)))))))
     
        
                          
                         
                       
                      
                         
                         
       
  
