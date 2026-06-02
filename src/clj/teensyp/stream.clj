(ns teensyp.stream
  "A namespace of utility functions for integrating streams into Teensyp."
  (:require [teensyp.buffer :as buf]
            [teensyp.concurrent :refer [with-lock]]
            [teensyp.server :as tcp])
  (:import [java.io IOException]
           [java.nio ByteBuffer]
           [java.util.concurrent CountDownLatch Executors ExecutorService]
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

(defn- blocking [f]
  (let [latch (CountDownLatch. 1)]
    (f #(.countDown latch))
    (.await latch)))

(defn- write! [sock buf] (blocking #(tcp/write sock buf %)))
(defn- close! [sock]     (blocking #(tcp/close sock %)))

(defn stream-handler
  "Create a Teensyp server handler from a function that takes an InputStream
  and OutputStream as arguments. Accepts an options map with the following keys:

  :executor - an executor for running the handler function, defaults to a fixed
              thread pool of 32 threads
  :read-buffer-size - the size in bytes of the read buffer, defaults to 8K

  The socket will be closed when both the InputStream and OutputStream are
  closed, or when the 'close' 2-arity of the returned function is called.
  Closing the OutputStream will prevent further writes, and closing the
  InputStream will prevent further reads. Data received after the InputStream
  has been closed will be silently dropped.

  Sometimes its useful for the client to indicate that the InputStream should
  be closed. If a nil buffer is passed to the 'read' 3-arity of the returned
  function, this will close the InputStream but not the OutputStream."
  ([handler]
   (stream-handler handler {}))
  ([handler {:keys [executor read-buffer-size] :or {read-buffer-size 8192}}]
   (fn
     ([socket]
      (let [executor   (or executor (new-default-executor))
            write-lock (ReentrantLock.)
            read-lock  (ReentrantLock.)
            can-read   (.newCondition read-lock)
            buffer     (.flip (ByteBuffer/allocate read-buffer-size))
            paused     (volatile! false)
            in-closed  (volatile! false)
            out-closed (volatile! false)
            readf      (fn [b off len]
                         (with-lock read-lock
                           (loop []
                             (cond
                              @in-closed -1
                              (not (.hasRemaining buffer))
                              (do (.await can-read) (recur))
                              :else
                              (let [len (min len (.remaining buffer))]
                                (.get buffer b off len)
                                (when @paused
                                  (vreset! paused false)
                                  (tcp/resume-reads socket))
                                (when (.hasRemaining buffer) (.signal can-read))
                                len)))))
            writef     (fn [b off len]
                         (with-lock write-lock
                           (if @out-closed
                             (throw (IOException. "Closed"))
                             (write! socket (ByteBuffer/wrap b off len)))))
            in-closef  (fn []
                         (with-lock read-lock
                           (with-lock write-lock
                             (vreset! in-closed true)
                             (.signal ^Condition can-read) 
                             (if (and @in-closed @out-closed)
                               (close! socket)
                               (when @paused
                                 (tcp/resume-reads socket))))))
            out-closef (fn []
                         (with-lock read-lock
                           (with-lock write-lock
                             (vreset! out-closed true)
                             (when (and @in-closed @out-closed)
                               (close! socket)))))
            input      (input-stream readf in-closef)
            output     (output-stream writef out-closef)]
        (.submit ^ExecutorService executor ^Runnable #(handler input output))
        {:buffer     buffer
         :can-read   can-read 
         :in-closed  in-closed
         :out-closed out-closed
         :paused     paused
         :read-lock  read-lock
         :write-lock write-lock}))
     ([{:keys [^ByteBuffer buffer can-read paused read-lock in-closed] :as state}
       socket ^ByteBuffer buf]
      (with-lock read-lock
        (cond
          (nil? buf) (do (vreset! in-closed true)
                         (.signal ^Condition can-read))
          @in-closed (.position buf (.limit buf))
          :else      (do (.compact buffer)
                         (buf/copy buf buffer)
                         (when-not (.hasRemaining buffer)
                           (vreset! paused true)
                           (tcp/pause-reads socket))
                         (.flip buffer)
                         (.signal ^Condition can-read)))
        state))
     ([{:keys [read-lock write-lock can-read in-closed out-closed]} _ex]
      (with-lock read-lock
        (with-lock write-lock
          (vreset! in-closed true)
          (vreset! out-closed true)
          (.signal ^Condition can-read)))))))
