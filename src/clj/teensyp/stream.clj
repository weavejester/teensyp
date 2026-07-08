(ns teensyp.stream
  "A namespace of utility functions for integrating streams into Teensyp."
  (:require [teensyp.buffer :as buf]
            [teensyp.concurrent :refer [with-lock]]
            [teensyp.server :as tcp])
  (:import [java.io IOException OutputStream]
           [java.nio ByteBuffer]
           [java.util.concurrent ExecutorService Executors]
           [java.util.concurrent.locks Condition LockSupport ReentrantLock]
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

(defn socket->output-stream
  "Create a blocking OutputStream from a TeensyP Socket. Writing to the stream
  will queue a write to the socket, and block until that write had been sent.

  Closing the stream will close the socket by default, but this behavior can be
  overridden by supplying a custom close function that takes the socket as
  its argument, via the :on-close option."
  (^OutputStream [socket]
   (socket->output-stream socket {}))
  (^OutputStream [socket {:keys [on-close]}]
   (let [lock     (ReentrantLock.)
         done     (volatile! false)
         closed   (volatile! false)
         blocking (fn [f]
                    (let [thread (Thread/currentThread)]
                      (f #(do (vreset! done true) (LockSupport/unpark thread)))
                      (while (not @done) (LockSupport/park))
                      (vreset! done false)))
         on-close (or on-close (fn [sock] (blocking #(tcp/close sock %))))]
     (output-stream
      (fn write [^bytes b off len]
        (with-lock lock
          (if @closed
            (throw (IOException. "OutputStream closed"))
            (blocking #(tcp/write socket (ByteBuffer/wrap b off len) %)))))
      (fn close []
        (with-lock lock
          (vreset! closed true)
          (on-close socket)))))))

(defn- new-default-executor []
  (Executors/newFixedThreadPool 32))

(defn input-stream-handler
  "Create a TeensyP server handler from a function f that takes an InputStream
  and a TeensyP Socket as arguments. The function will be executed in a
  separate thread when the 1-argument accept arity of the handler is called.
  
  Accepts an options map with the following keys:

  :executor - an executor for running the supplied function, defaults to a
              fixed thread pool of 32 threads
  :on-close - a function called when the InputStream is closed, takes a
              single socket argument
  :read-buffer-size - the size in bytes of the read buffer, defaults to 8K

  Triggering the 2-argument close arity of the handler will close the
  associated InputStream."
  ([f] (input-stream-handler f {}))
  ([f {:keys [executor on-close read-buffer-size]
       :or {executor         (new-default-executor)
            on-close         (fn [_sock])
            read-buffer-size 8192}}]
   (fn
     ([socket]
      (let [lock     (ReentrantLock.)
            can-read (.newCondition lock)
            paused   (volatile! false)
            closed   (volatile! false)
            buffer   (.flip (ByteBuffer/allocate read-buffer-size))
            readf    (fn [b off ^long len]
                       (with-lock lock
                         (loop []
                           (cond
                             (.hasRemaining buffer)
                             (let [len (min len (.remaining buffer))]
                               (.get buffer b off len)
                               (when @paused
                                 (vreset! paused false)
                                 (tcp/resume-reads socket))
                               (when (.hasRemaining buffer)
                                 (.signal can-read))
                               len)
                             @closed -1
                             :else   (do (.await can-read) (recur))))))
            closef   (fn []
                       (with-lock lock
                         (vreset! closed true)
                         (on-close socket)
                         (.signal ^Condition can-read)))
            stream   (input-stream readf closef)]
        (.submit ^ExecutorService executor ^Runnable #(f stream socket))
        {:buffer   buffer
         :can-read can-read
         :closed   closed
         :lock     lock
         :paused   paused}))
     ([{:keys [^ByteBuffer buffer can-read paused lock closed] :as state}
       socket ^ByteBuffer buf]
      (with-lock lock
        (if @closed
          (.position buf (.limit buf))
          (do (.compact buffer)
              (buf/copy buf buffer)
              (when-not (.hasRemaining buffer)
                (vreset! paused true)
                (tcp/pause-reads socket))
              (.flip buffer)
              (.signal ^Condition can-read)))
        state))
     ([{:keys [can-read lock closed]} _exception]
      (with-lock lock
        (vreset! closed true)
        (.signal ^Condition can-read))))))

(defn stream-handler
  "Create a Teensyp server handler from a function that takes an InputStream
  and OutputStream as arguments. This combines the [[socket->output-stream]]
  and [[input-stream-handler]] functions.
  
  Accepts an options map with the following keys:

  :executor - an executor for running the handler function, defaults to a fixed
              thread pool of 32 threads
  :read-buffer-size - the size in bytes of the read buffer, defaults to 8K

  The socket will be closed when both the InputStream and OutputStream are
  closed, or when the 'close' 2-arity of the returned function is called.
  Closing the OutputStream will prevent further writes, and closing the
  InputStream will prevent further reads. Data received after the InputStream
  has been closed will be silently dropped."
  ([f]
   (stream-handler f {}))
  ([f options]
   (let [closed       (atom 0)
         on-close-out #(when (= 3 (swap! closed bit-or 1)) (tcp/close %))
         on-close-in  #(when (= 3 (swap! closed bit-or 2)) (tcp/close %))]
     (input-stream-handler
      (fn [in sock]
        (f in (socket->output-stream sock {:on-close on-close-out})))
      (assoc options :on-close on-close-in)))))
