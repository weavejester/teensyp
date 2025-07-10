(ns teensyp.server-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [teensyp.server :as tcp])
  (:import [java.net Socket]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(deftest server-close-test
  (with-open [server (tcp/start-server {:port 3456})]
    (is (instance? java.io.Closeable server))))

(defn- string-write [s write]
  (write (ByteBuffer/wrap (.getBytes s StandardCharsets/UTF_8))))

(defn- hello-handler [_ write]
  (write "hello\n")
  (write tcp/closed))

(deftest server-write-test
  (with-open [_ (tcp/start-server
                 {:port  3457
                  :watch hello-handler
                  :write string-write})]
    (let [sock (Socket. "localhost" 3457)]
      (with-open [reader (io/reader (.getInputStream sock))]
        (is (= "hello" (.readLine reader)))))))

(defn- string-read [state buf]
  (let [b (byte-array (.remaining buf))]
    (.get buf b)
    (update state :data str (String. b StandardCharsets/UTF_8))))

(defn- echo-handler [state write]
  (some-> (:data state) write)
  (dissoc state :data))

(deftest server-echo-test
  (with-open [_ (tcp/start-server
                 {:port  3458
                  :watch echo-handler
                  :write string-write
                  :read  string-read})]
    (let [sock (Socket. "localhost" 3458)]
      (with-open [writer (io/writer (.getOutputStream sock))]
        (with-open [reader (io/reader (.getInputStream sock))]
          (doto writer (.write "foo\n") .flush)
          (is (= "foo" (.readLine reader)))
          (doto writer (.write "bar\n") .flush)
          (is (= "bar" (.readLine reader)))
          (doto writer (.write "foo") (.write "bar\n") .flush)
          (is (= "foobar" (.readLine reader))))))))

(deftest server-socket-close-test
  (let [closed? (promise)]
    (with-open [_ (tcp/start-server
                   {:port  3459
                    :watch (fn [_ _])
                    :close (fn [_ _] (deliver closed? true))})]
      (.close (Socket. "localhost" 3459))
      (is (deref closed? 100 false)))))
