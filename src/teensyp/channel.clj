(ns teensyp.channel
  (:require [teensyp.buffer :as buf])
  (:import [java.nio ByteBuffer]
           [java.nio.channels AsynchronousByteChannel Channel
            ClosedChannelException CompletionHandler
            ReadPendingException WritePendingException]
           [java.util.concurrent CompletableFuture Future]))

(deftype FutureCompletionHandler [^CompletableFuture fut]
  CompletionHandler
  (completed [_ result _] (.complete fut result))
  (failed [_ ex _] (.completeExceptionally fut ex)))

(deftype AsyncByteBufferChannel
         [^:volatile-mutable ^ByteBuffer buffer
          ^:volatile-mutable pending-read
          ^:volatile-mutable pending-write
          ^:volatile-mutable open?]
  AsynchronousByteChannel
  (^Future read [this ^ByteBuffer buf]
    (let [fut (CompletableFuture.)]
      (.read this buf nil (FutureCompletionHandler. fut))
      fut))
  (^void read [this ^ByteBuffer buf att ^CompletionHandler handler]
    (letfn [(read-buffer []
              (locking this
                (let [num (buf/copy (.buffer this) buf)
                      rem (-> this .buffer .remaining)]
                  (when (zero? rem)
                    (set! (.buffer this) nil))
                  (.completed handler num att)
                  (when (and pending-write (zero? rem))
                    (pending-write)
                    (set! (.pending-write this) nil)))))]
      (locking this
        (when-not open? (throw (ClosedChannelException.)))
        (if (nil? buffer)
          (if (nil? pending-read)
            (set! pending-read read-buffer)
            (throw (ReadPendingException.)))
          (read-buffer)))))
  (^Future write [this ^ByteBuffer buf]
    (let [fut (CompletableFuture.)]
      (.write this buf nil (FutureCompletionHandler. fut))
      fut))
  (^void write [this ^ByteBuffer buf att ^CompletionHandler handler]
    (letfn [(write-buffer []
              (locking this
                (set! (.buffer this) buf)
                (.completed handler (.remaining buf) att)
                (when pending-read
                  (pending-read)
                  (set! (.pending-read this) nil))))]
      (locking this
        (when-not open? (throw (ClosedChannelException.)))
        (if (nil? buffer)
          (write-buffer)
          (if (nil? pending-write)
            (set! pending-write write-buffer)
            (throw (WritePendingException.)))))))
  Channel
  (isOpen [_] open?)
  (close [_] (set! open? false)))

(defn buffer-channel ^AsynchronousByteChannel []
  (AsyncByteBufferChannel. nil nil nil true))
