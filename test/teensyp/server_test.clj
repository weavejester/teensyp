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

(defn ->buffer [^String s]
  (ByteBuffer/wrap (.getBytes s StandardCharsets/UTF_8)))

(defn- hello-handler
  ([_state write]
   (write (->buffer "hello\n"))
   (write tcp/closed))
  ([_state _buffer _write])
  ([_state]))

(deftest server-write-test
  (with-open [_ (tcp/start-server
                 {:port  3457
                  :handler hello-handler})]
    (let [sock (Socket. "localhost" 3457)]
      (with-open [reader (io/reader (.getInputStream sock))]
        (is (= "hello" (.readLine reader)))))))

(defn- <-buffer [^ByteBuffer buf]
  (let [b (byte-array (.remaining buf))]
    (.get buf b)
    (String. b StandardCharsets/UTF_8)))

(defn- echo-handler
  ([_state _write])
  ([_state buffer write]
   (let [s (<-buffer buffer)]
     (write (->buffer s))))
  ([_state]))

(deftest server-echo-test
  (with-open [_ (tcp/start-server
                 {:port  3458
                  :handler echo-handler})]
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
                    :handler
                    (fn
                      ([_ _])
                      ([_ _ _])
                      ([_] (deliver closed? true)))})]
      (.close (Socket. "localhost" 3459))
      (is (deref closed? 100 false)))))
