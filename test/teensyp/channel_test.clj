(ns teensyp.channel-test
  (:require [clojure.test :refer [deftest is]]
            [teensyp.channel :as ch])
  (:import [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(def ascii StandardCharsets/US_ASCII)

(deftest close-channel-test
  (let [ch (ch/buffer-channel)]
    (is (.isOpen ch))
    (.close ch)
    (is (not (.isOpen ch)))))

(deftest future-channel-test
  (let [ch  (ch/buffer-channel)
        fut (.write ch (ByteBuffer/wrap (.getBytes "hello" ascii)))]
    (is (.isDone fut))
    (is (= (.get fut) 5))
    (let [buf (ByteBuffer/allocate 5)
          fut (.read ch buf)]
      (is (.isDone fut))
      (is (= (.get fut) 5))
      (is (= "hello" (String. (.array buf) ascii))))))
