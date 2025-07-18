(ns teensyp.channel
  (:require [teensyp.buffer :as buf])
  (:import [java.nio ByteBuffer]
           [java.nio.channels AsynchronousByteChannel Channel
            ClosedChannelException CompletionHandler
            ReadPendingException WritePendingException]
           [java.util.concurrent Future FutureTask]))

(deftype AsyncByteBufferChannel
         [^:volatile-mutable ^ByteBuffer buffer
          ^:volatile-mutable pending-read
          ^:volatile-mutable pending-write
          ^:volatile-mutable open?]
  AsynchronousByteChannel
  (^Future read [this ^ByteBuffer buf]
    (let [fut (FutureTask.
               #(locking this
                  (let [num (buf/copy buffer buf)]
                    (when-not (.hasRemaining buffer)
                      (set! (.buffer this) nil))
                    (when (and pending-write (nil? buffer))
                      (pending-write))
                    num)))]
      (locking this
        (when-not open? (throw (ClosedChannelException.)))
        (if (nil? buffer)
          (if (nil? pending-read)
            (set! pending-read #(.run fut))
            (throw (ReadPendingException.)))
          (.run fut))
        fut)))
  (^void read [this ^ByteBuffer buf att ^CompletionHandler handler]
    (letfn [(read-buffer []
              (locking this
                (let [num (buf/copy buffer buf)]
                  (when-not (.hasRemaining buffer)
                    (set! (.buffer this) nil))
                  (when (and pending-write (nil? buffer))
                    (pending-write))
                  (.completed handler num att))))]
      (locking this
        (when-not open? (throw (ClosedChannelException.)))
        (if (nil? buffer)
          (if (nil? pending-read)
            (set! pending-read buf)
            (throw (ReadPendingException.)))
          (read-buffer)))))
  (^Future write [this ^ByteBuffer buf]
   (let [fut (FutureTask.
              #(locking this
                 (set! (.buffer this) buf)
                 (when pending-read (pending-read))
                 (.remaining buf)))]
     (locking this
       (when-not open? (throw (ClosedChannelException.)))
       (if (nil? buffer)
         (.run fut)
         (if (nil? pending-write)
           (set! pending-write #(.run fut))
           (throw (WritePendingException.))))
       fut)))
  (^void write [this ^ByteBuffer buf att ^CompletionHandler handler]
   (letfn [(write-buffer []
             (locking this
               (set! (.buffer this) buf)
               (.completed handler (.remaining buf) att)
               (when pending-read (pending-read))))]
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
