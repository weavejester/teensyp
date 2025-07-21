(ns teensyp.channel-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [teensyp.channel :as ch])
  (:import [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.nio.channels AsynchronousByteChannel ClosedChannelException]
           [java.util.concurrent ExecutionException TimeUnit]))

(def ascii StandardCharsets/US_ASCII)

(defn- write-str [^AsynchronousByteChannel ch s]
  (.write ch (ByteBuffer/wrap (.getBytes s ascii))))

(deftest close-channel-test
  (let [ch  (ch/async-channel)
        buf (ByteBuffer/allocate 5)]
    (is (.isOpen ch))
    (.close ch)
    (is (not (.isOpen ch)))
    (is (thrown? ClosedChannelException (.write ch buf)))
    (is (thrown? ClosedChannelException (.read ch buf)))))

(deftest close-channel-future-test
  (testing "reads"
    (let [ch  (ch/async-channel)
          buf (ByteBuffer/allocate 5)
          fut (.read ch buf)]
      (.close ch)
      (is (thrown? ExecutionException (.get fut)))))
  (testing "writes"
    (let [ch   (ch/async-channel)
          fut1 (write-str ch "hello")
          fut2 (write-str ch "world")]
      (is (.isDone fut1))
      (is (= 5 (.get fut1 1 TimeUnit/SECONDS)))
      (.close ch)
      (is (thrown? ExecutionException (.get fut2))))))

(deftest future-channel-test
  (let [ch  (ch/async-channel)
        fut (write-str ch "hello")]
    (is (.isDone fut))
    (is (= (.get fut) 5))
    (let [buf (ByteBuffer/allocate 5)
          fut (.read ch buf)]
      (is (.isDone fut))
      (is (= 5 (.get fut 1 TimeUnit/SECONDS)))
      (is (= [104 101 108 108 111] (seq (.array buf)))))))

(deftest partial-read-test
  (let [ch (ch/async-channel)]
    (write-str ch "hello")
    (let [buf (ByteBuffer/allocate 3)
          fut (.read ch buf)]
      (is (.isDone fut))
      (is (= 3 (.get fut 1 TimeUnit/SECONDS)))
      (is (= [104 101 108] (seq (.array buf)))))
    (let [buf (ByteBuffer/allocate 3)
          fut (.read ch buf)]
      (is (.isDone fut))
      (is (= 2 (.get fut 1 TimeUnit/SECONDS)))
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

(deftest future-read-wait-test
  (let [ch  (ch/async-channel)
        buf (ByteBuffer/allocate 5)
        fut (.read ch buf)]
    (is (not (.isDone fut)))
    (write-str ch "hello")
    (is (.isDone fut))
    (is (= 5 (.get fut 1 TimeUnit/SECONDS)))
    (is (= [104 101 108 108 111] (seq (.array buf))))))

(deftest read-wait-test
  (let [ch    (ch/async-channel)
        rbuf  (ByteBuffer/allocate 5)
        rdone (promise)
        rfail (promise)
        wdone (promise)
        wfail (promise)]
    (ch/read ch rbuf rdone rfail)
    (is (not (realized? rdone)))
    (is (not (realized? rfail)))
    (ch/write ch (ByteBuffer/wrap (.getBytes "hello" ascii)) wdone wfail)
    (is (realized? wdone))
    (is (realized? rdone))
    (is (not (realized? wfail)))
    (is (not (realized? rfail)))
    (is (= 5 (deref wdone 1000 :fail)))
    (is (= 5 (deref rdone 1000 :fail)))
    (is (= [104 101 108 108 111] (seq (.array rbuf))))))

(deftest multi-thread-test
  (let [ch   (ch/async-channel)
        buf  (ByteBuffer/allocate 4096)
        tin  (Thread. #(dotimes [i 1024]
                         (.get (write-str ch (str i)) 1 TimeUnit/SECONDS)))
        tout (Thread. #(dotimes [_ 1024]
                         (.get (.read ch buf) 1 TimeUnit/SECONDS)))]
    (.start tin)
    (.start tout)
    (.join tin 4000)
    (.join tout 4000)
    (.flip buf)
    (let [bs (byte-array (.remaining buf))]
      (.get buf bs)
      (is (= (apply str (range 1024))
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
    (.join tin 1000)
    (.join tout 1000)
    (is (= (apply str (range 128))
           (.toString sb)))))
