(ns teensyp.buffer
  (:refer-clojure :exclude [read-line])
  (:import [java.nio ByteBuffer]
           [java.nio.charset Charset StandardCharsets]))

(defn buffer
  "Create a ByteBuffer of the assigned size."
  ^ByteBuffer [size]
  (ByteBuffer/allocate size))

(defn str->buffer
  "Convert a String into a ByteBuffer."
  ^ByteBuffer [^String s ^Charset charset]
  (ByteBuffer/wrap (.getBytes s charset)))

(defn index-of
  "Find the first index of the specified byte in a buffer."
  [^ByteBuffer buffer needle]
  (let [len (.remaining buffer)]
    (loop [index 0]
      (if (< index len)
        (if (= needle (.get buffer index))
          index
          (recur (inc index)))
        -1))))

(defn- matches-tail-bytes? [^ByteBuffer buffer index ^bytes needle]
  (loop [i (inc index), j 1]
    (if (< j (alength needle))
      (when (= (.get buffer i) (aget needle j))
        (recur (inc i) (inc j)))
      true)))

(defn index-of-array
  "Find the first index of the specified array of bytes within a buffer."
  [^ByteBuffer buffer ^bytes needle]
  (if (zero? (alength needle))
    -1
    (let [b   (aget needle 0)
          end (- (.remaining buffer) (alength needle) 1)]
      (loop [i 0]
        (if (and (= b (.get buffer i))
                 (matches-tail-bytes? buffer i needle))
          i
          (if (< i end)
            (recur (inc i))
            -1))))))

(defn read-line
  "Return the next line from the buffer, or nil if the buffer has no LF. Strips
  ending CR and LF characters from the resulting string. If a string is
  returned, the buffer's position is advanced accordingly."
  [^ByteBuffer buffer ^Charset charset]
  (let [CR 0x0D, LF 0x0A
        index (index-of buffer LF)]
    (when (not= index -1)
      (let [len (if (= CR (.get buffer (dec index)))
                  (dec index)
                  index)
            bs  (byte-array len)]
        (.get buffer bs)
        (.position buffer (inc index))
        (String. bs charset)))))

(defn copy
  "Copy the source buffer into the destination buffer, and increment the
  positions of both buffers. If the source buffer has more remaining than the
  destination, as much as possible will be copied until the destination is
  full."
  [^ByteBuffer src ^ByteBuffer dest]
  (when (and (.hasRemaining src) (.hasRemaining dest))
    (let [diff (- (.remaining src) (.remaining dest))]
      (if (pos? diff)
        (let [limit (.limit src)]
          (.limit src (- limit diff))
          (.put dest src)
          (.limit src limit))
        (.put dest src)))))
