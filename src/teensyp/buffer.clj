(ns teensyp.buffer
  (:import [java.nio ByteBuffer]
           [java.nio.charset Charset StandardCharsets]))

(defn buffer
  "Create a ByteBuffer of the assigned size."
  ^ByteBuffer [size]
  (ByteBuffer/allocate size))

(defn str->buffer
  "Convert a String into a ByteBuffer."
  (^ByteBuffer [^String s]
   (str->buffer s StandardCharsets/UTF_8))
  (^ByteBuffer [^String s ^Charset charset]
   (ByteBuffer/wrap (.getBytes s charset))))

(defn- matches-bytes? [^ByteBuffer buffer index ^bytes needle]
  (loop [i (inc index), j 1]
    (if (< j (alength needle))
      (when (= (.get buffer i) (aget needle j))
        (recur (inc i) (inc j)))
      true)))

(defn index-of-bytes
  "Find an array of bytes within a buffer."
  [^ByteBuffer buffer ^bytes needle]
  (if (zero? (alength needle))
    -1
    (let [b   (aget needle 0)
          end (- (.remaining buffer) (alength needle) 1)]
      (loop [i 0]
        (if (and (= b (.get buffer i))
                 (matches-bytes? buffer i needle))
          i
          (if (< i end)
            (recur (inc i))
            -1))))))
