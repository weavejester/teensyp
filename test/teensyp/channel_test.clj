(ns teensyp.channel-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [teensyp.channel :as ch])
  (:import [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.nio.channels AsynchronousByteChannel]
           [java.util.concurrent TimeUnit]))

(def ascii StandardCharsets/US_ASCII)

(defn- write-str [^AsynchronousByteChannel ch s]
  (.write ch (ByteBuffer/wrap (.getBytes s ascii))))

(deftest close-channel-test
  (let [ch (ch/async-channel)]
    (is (.isOpen ch))
    (.close ch)
    (is (not (.isOpen ch)))))

(deftest future-channel-test
  (let [ch  (ch/async-channel)
        fut (write-str ch "hello")]
    (is (.isDone fut))
    (is (= (.get fut) 5))
    (let [buf (ByteBuffer/allocate 5)
          fut (.read ch buf)]
      (is (.isDone fut))
      (is (= 5 (.get fut)))
      (is (= [104 101 108 108 111] (seq (.array buf)))))))

(deftest partial-read-test
  (let [ch (ch/async-channel)]
    (write-str ch "hello")
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
  (let [ch (ch/async-channel)]
    (write-str ch "hello")
    (.read ch (ByteBuffer/allocate 3))
    (let [fut (write-str ch "world")]
      (is (not (.isDone fut)))
      (.read ch (ByteBuffer/allocate 2))
      (is (.isDone fut))
      (is (= 5 (.get fut 1 TimeUnit/SECONDS))))))

(deftest read-wait-test
  (let [ch  (ch/async-channel)
        buf (ByteBuffer/allocate 5)
        fut (.read ch buf)]
    (is (not (.isDone fut)))
    (write-str ch "hello")
    (is (.isDone fut))
    (is (= 5 (.get fut 1 TimeUnit/SECONDS)))
    (is (= [104 101 108 108 111] (seq (.array buf))))))

(deftest multi-thread-test
  (let [ch   (ch/async-channel)
        buf  (ByteBuffer/allocate 2048)
        tin  (Thread. #(dotimes [i 128]
                         (.get (write-str ch (str i)))))
        tout (Thread. #(dotimes [_ 128]
                         (.get (.read ch buf))))]
    (.start tin)
    (.start tout)
    (.join tin)
    (.join tout)
    (.flip buf)
    (let [bs (byte-array (.remaining buf))]
      (.get buf bs)
      (is (= (apply str (range 128))
             (String. bs ascii))))))

(deftest streaming-test
  (let [ch   (ch/async-channel)
        in   (ch/->input-stream ch)
        out  (ch/->output-stream ch)
        sb   (StringBuilder.)
        tout (Thread. #(let [w (io/writer out)]
                         (dotimes [i 128]
                           (.write w (str i "\n")))
                         (.flush w)))
        tin  (Thread. #(let [r (io/reader in)]
                         (dotimes [_ 128]
                           (.append sb (.readLine r)))))]
    (.start tin)
    (.start tout)
    (.join tin)
    (.join tout)
    (is (= (apply str (range 128))
           (.toString sb)))))
