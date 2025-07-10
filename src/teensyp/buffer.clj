(ns teensyp.buffer
  (:import [java.nio ByteBuffer]
           [java.nio.charset Charset StandardCharsets]))

(defn buffer
  "Create a ByteBuffer of the assigned size."
  [size]
  (ByteBuffer/allocate size))

(defn str->buffer
  "Convert a String into a ByteBuffer."
  ([^String s]
   (str->buffer s StandardCharsets/UTF_8))
  ([^String s ^Charset charset]
   (ByteBuffer/wrap (.getBytes s charset))))
