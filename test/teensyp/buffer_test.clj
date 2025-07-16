(ns teensyp.buffer-test
  (:require [clojure.test :refer [deftest is]]
            [teensyp.buffer :as buf])
  (:import [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(def ascii StandardCharsets/US_ASCII)

(deftest buffer-test
  (is (instance? ByteBuffer (buf/buffer 128))))

(deftest str->buffer-test
  (is (= [104 101 108 108 111]
         (seq (.array (buf/str->buffer "hello" ascii))))))

(deftest index-of-test
  (let [bs (byte-array [104 101 108 108 111])]
    (is (= 2 (-> (ByteBuffer/wrap bs) (buf/index-of 108))))
    (is (= -1 (-> (ByteBuffer/wrap bs) (buf/index-of 112))))
    (is (= 3 (-> (ByteBuffer/wrap bs)
                 (doto (.position 3))
                 (buf/index-of 108))))))

(deftest index-of-array-test
  (let [bs (byte-array [104 101 108 108 111 104 101 108 108 111])]
    (is (= 2 (-> (ByteBuffer/wrap bs)
                 (buf/index-of-array (byte-array [108 108])))))
    (is (= -1 (-> (ByteBuffer/wrap bs)
                  (buf/index-of-array (byte-array [111 108])))))
    (is (= 7 (-> (ByteBuffer/wrap bs)
                 (doto (.position 5))
                 (buf/index-of-array (byte-array [108 108])))))))

(deftest read-line-test
  (let [buf (ByteBuffer/wrap (.getBytes "hello\nworld\n" ascii))]
    (is (= "hello" (buf/read-line buf ascii)))
    (is (= 6 (.position buf))))
  (let [buf (ByteBuffer/wrap (.getBytes "hello\r\nworld\r\n" ascii))]
    (is (= "hello" (buf/read-line buf ascii)))
    (is (= 7 (.position buf))))
  (let [buf (ByteBuffer/wrap (.getBytes "hello\n" ascii))]
    (is (= "hello" (buf/read-line buf ascii)))
    (is (= 6 (.position buf))))
  (let [buf (ByteBuffer/wrap (.getBytes "hello" ascii))]
    (is (nil? (buf/read-line buf ascii)))
    (is (= 0 (.position buf))))
  (let [buf (ByteBuffer/wrap (.getBytes "a\nb\nc\nd" ascii))]
    (is (= "a" (buf/read-line buf ascii)))
    (is (= 2 (.position buf)))
    (is (= "b" (buf/read-line buf ascii)))
    (is (= 4 (.position buf)))
    (is (= "c" (buf/read-line buf ascii)))
    (is (= 6 (.position buf)))
    (is (nil? (buf/read-line buf ascii)))
    (is (= 6 (.position buf)))))

(deftest copy-test
  (let [buf (buf/buffer 5)]
    (buf/copy (buf/str->buffer "hello" ascii) buf)
    (is (= [104 101 108 108 111] (seq (.array buf)))))
  (let [buf (buf/buffer 5)]
    (buf/copy (buf/str->buffer "hello:world" ascii) buf)
    (is (= [104 101 108 108 111] (seq (.array buf)))))
  (let [buf (buf/buffer 5)]
    (buf/copy (buf/str->buffer "hel" ascii) buf)
    (is (= [104 101 108 0 0] (seq (.array buf))))
    (buf/copy (buf/str->buffer "lo:world" ascii) buf)
    (is (= [104 101 108 108 111] (seq (.array buf))))))
