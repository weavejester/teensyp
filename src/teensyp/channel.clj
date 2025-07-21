(ns teensyp.channel
  "A namespace of utility functions for interoperating with Java channels and
  streams."
  (:refer-clojure :exclude [read])
  (:require [teensyp.buffer :as buf])
  (:import [java.io InputStream OutputStream]
           [java.nio ByteBuffer]
           [java.nio.channels AsynchronousByteChannel Channel
            Channels ClosedChannelException CompletionHandler
            ReadPendingException WritePendingException]
           [java.util.concurrent CompletableFuture Future]))

(defn- future-handler ^CompletionHandler [^CompletableFuture fut]
  (reify CompletionHandler
    (completed [_ result _] (.complete fut result))
    (failed [_ ex _] (.completeExceptionally fut ex))))

(defn- fn-handler ^CompletionHandler [complete fail]
  (reify CompletionHandler
    (completed [_ result _] (complete result))
    (failed [_ ex _] (fail ex))))

(deftype AsyncChannelImpl
         [^:volatile-mutable ^ByteBuffer buffer
          ^:volatile-mutable pending-read
          ^:volatile-mutable pending-write
          ^:volatile-mutable closed?]
  AsynchronousByteChannel
  (^Future read [this ^ByteBuffer buf]
    (let [fut (CompletableFuture.)]
      (.read this buf nil (future-handler fut))
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
      (.write this buf nil (future-handler fut))
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

(defn async-channel
  "Create a channel object satisfying the `AsynchronousByteChannel` interface.
  ByteBuffers written to this channel with the `write` method can be later
  read back with the `read` method.

  Only one write and one read can be pending. Attempting to write or read
  from the channel when there are operations pending will result in a
  `WritePendingException` or `ReadPendingException` respectively.

  When a buffer is passed to the `write` method, it is owned by the channel
  until it is fully read. This avoids the use of an intermediate buffer, but
  care needs to be taken around the reuse of buffers."
  ^AsynchronousByteChannel []
  (AsyncChannelImpl. nil nil nil false))

(defn read
  "Asynchronously read from an `AsynchronousByteChannel`, putting the result
  in buf, and calling either the complete or fail callback."
  [^AsynchronousByteChannel ch ^ByteBuffer buf complete fail]
  (.read ch buf nil (fn-handler complete fail)))

(defn write
  "Asynchronously write to an `AsynchronousByteChannel`, putting the result
  in buf, and calling either the complete or fail callback."
  [^AsynchronousByteChannel ch ^ByteBuffer buf complete fail]
  (.write ch buf nil (fn-handler complete fail)))

(defn ->input-stream
  "Convert an `AsynchronousByteChannel` into a blocking `InputStream` instance."
  ^InputStream [^AsynchronousByteChannel ch]
  (Channels/newInputStream ch))

(defn ->output-stream
  "Convert an `AsynchronousByteChannel` into a blocking `OutputStream`
  instance."
  ^OutputStream [^AsynchronousByteChannel ch]
  (Channels/newOutputStream ch))
