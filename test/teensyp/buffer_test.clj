(ns teensyp.buffer-test
  (:require [clojure.test :refer [deftest is]]
            [teensyp.buffer :as buf])
  (:import [java.nio ByteBuffer]))

(deftest buffer-test
  (is (instance? ByteBuffer (buf/buffer 128))))

(deftest str->buffer-test
  (is (= [104 101 108 108 111]
         (seq (.array (buf/str->buffer "hello"))))))
