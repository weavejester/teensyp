(ns teensyp.channel-test
  (:require [clojure.test :refer [deftest is]]
            [teensyp.channel :as ch])
  (:import [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.util.concurrent TimeUnit]))

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
      (is (= 5 (.get fut)))
      (is (= [104 101 108 108 111] (seq (.array buf)))))))

(deftest partial-read-test
  (let [ch (ch/buffer-channel)]
    (.write ch (ByteBuffer/wrap (.getBytes "hello" ascii)))
    (let [buf (ByteBuffer/allocate 3)
          fut (.read ch buf)]
      (is (.isDone fut))
      (is (= 3 (.get fut)))
      (is (= [104 101 108] (seq (.array buf)))))
    (let [buf (ByteBuffer/allocate 3)
          fut (.read ch buf)]
      (is (.isDone fut))
      (is (= 2 (.get fut)))
      (is (= [108 111 0] (seq (.array buf)))))))

(deftest write-wait-test
  (let [ch (ch/buffer-channel)]
    (.write ch (ByteBuffer/wrap (.getBytes "hello" ascii)))
    (.read ch (ByteBuffer/allocate 3))
    (let [fut (.write ch (ByteBuffer/wrap (.getBytes "world" ascii)))]
      (is (not (.isDone fut)))
      (.read ch (ByteBuffer/allocate 2))
      (is (.isDone fut))
      (is (= 5 (.get fut 1 TimeUnit/SECONDS))))))
