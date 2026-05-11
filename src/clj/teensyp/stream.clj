(ns teensyp.stream
  (:import [teensyp IInputStream IOutputStream
            ProxyInputStream ProxyOutputStream]))

(defn input-stream
  ([readf]
   (input-stream readf (fn [])))
  ([readf closef]
   (ProxyInputStream.
    (reify IInputStream
      (read [_ b off len] (readf b off len))
      (close [_] (closef))))))

(defn output-stream
  ([writef]
   (output-stream writef (fn [])))
  ([writef closef]
   (output-stream writef closef (fn [])))
  ([writef closef flushf]
   (ProxyOutputStream.
    (reify IOutputStream
      (write [_ b off len] (writef b off len))
      (close [_] (closef))
      (flush [_] (flushf))))))
