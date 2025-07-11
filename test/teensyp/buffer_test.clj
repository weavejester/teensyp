(ns teensyp.buffer-test
  (:require [clojure.test :refer [deftest is]]
            [teensyp.buffer :as buf])
  (:import [java.nio ByteBuffer]))

(deftest buffer-test
  (is (instance? ByteBuffer (buf/buffer 128))))

(deftest str->buffer-test
  (is (= [104 101 108 108 111]
         (seq (.array (buf/str->buffer "hello"))))))

(deftest index-of-test
  (let [bs (byte-array [104 101 108 108 111])]
    (is (= 2 (-> (ByteBuffer/wrap bs) (buf/index-of 108))))
    (is (= -1 (-> (ByteBuffer/wrap bs) (buf/index-of 112))))))

(deftest index-of-array-test
  (let [bs (byte-array [104 101 108 108 111])]
    (is (= 2 (-> (ByteBuffer/wrap bs)
                 (buf/index-of-array (byte-array [108 108])))))
    (is (= -1 (-> (ByteBuffer/wrap bs)
                  (buf/index-of-array (byte-array [111 108])))))))
