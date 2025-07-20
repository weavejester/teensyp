(ns teensyp.channel
  (:require [teensyp.buffer :as buf])
  (:import [java.io InputStream OutputStream]
           [java.nio ByteBuffer]
           [java.nio.channels AsynchronousByteChannel Channel
            Channels ClosedChannelException CompletionHandler
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
          ^:volatile-mutable closed?]
  AsynchronousByteChannel
  (^Future read [this ^ByteBuffer buf]
    (let [fut (CompletableFuture.)]
      (.read this buf nil (FutureCompletionHandler. fut))
      fut))
  (^void read [this ^ByteBuffer buf att ^CompletionHandler handler]
    (letfn [(read-buffer []
              (locking this
                (if (.closed? this)
                  (.failed handler (ClosedChannelException.) nil)
                  (let [num (buf/copy (.buffer this) buf)
                        rem (-> this .buffer .remaining)]
                    (when (zero? rem)
                      (set! (.buffer this) nil))
                    (.completed handler (int num) att)
                    (when-some [pending-write (.pending-write this)]
                      (when (zero? rem)
                        (pending-write)
                        (set! (.pending-write this) nil)))))))]
      (locking this
        (when closed? (throw (ClosedChannelException.)))
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
                (if (.closed? this)
                  (.failed handler (ClosedChannelException.) nil)
                  (do (set! (.buffer this) buf)
                      (.completed handler (-> buf .remaining int) att)
                      (when-some [pending-read (.pending-read this)]
                        (pending-read)
                        (set! (.pending-read this) nil))))))]
      (locking this
        (when closed? (throw (ClosedChannelException.)))
        (if (nil? buffer)
          (write-buffer)
          (if (nil? pending-write)
            (set! pending-write write-buffer)
            (throw (WritePendingException.)))))))
  Channel
  (isOpen [_] (not closed?))
  (close [_]
    (set! closed? true)
    (when pending-write (pending-write))
    (when pending-read  (pending-read))))

(defn async-channel ^AsynchronousByteChannel []
  (AsyncByteBufferChannel. nil nil nil false))

(defn ->input-stream ^InputStream [^AsynchronousByteChannel ch]
  (Channels/newInputStream ch))

(defn ->output-stream ^OutputStream [^AsynchronousByteChannel ch]
  (Channels/newOutputStream ch))
