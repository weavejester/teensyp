(ns teensyp.server-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [teensyp.server :as tcp]
            [teensyp.buffer :as buf])
  (:import [java.net Socket]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(deftest server-close-test
  (with-open [server (tcp/start-server {:port 3456})]
    (is (instance? java.io.Closeable server))))

(defn- hello-handler
  ([write]
   (write (buf/str->buffer "hello\n"))
   (write tcp/closed))
  ([_state _buffer _write])
  ([_state _exception]))

(deftest server-write-test
  (with-open [_ (tcp/start-server
                 {:port 3457
                  :handler hello-handler})]
    (let [sock (Socket. "localhost" 3457)]
      (with-open [reader (io/reader (.getInputStream sock))]
        (is (= "hello" (.readLine reader)))))))

(defn- <-buffer [^ByteBuffer buf]
  (let [b (byte-array (.remaining buf))]
    (.get buf b)
    (String. b StandardCharsets/UTF_8)))

(defn- echo-handler
  ([_write])
  ([_state buffer write]
   (let [s (<-buffer buffer)]
     (write (buf/str->buffer s))))
  ([_state _exception]))

(deftest server-echo-test
  (with-open [_ (tcp/start-server
                 {:port 3458
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
                   {:port 3459
                    :handler
                    (fn
                      ([_])
                      ([_ _ _])
                      ([_ _] (deliver closed? true)))})]
      (.close (Socket. "localhost" 3459))
      (is (deref closed? 100 false)))))

(defn- reverse-line-handler
  ([_write])
  ([_state buffer write]
   (when-some [line (buf/read-line buffer StandardCharsets/US_ASCII)]
     (-> (apply str (reverse line))
         (str "\r\n")
         (buf/str->buffer StandardCharsets/US_ASCII)
         (write))))
  ([_state _ex]))

(deftest server-readline-test
  (with-open [_ (tcp/start-server
                 {:port 3460
                  :handler reverse-line-handler})]
    (let [sock (Socket. "localhost" 3460)]
      (with-open [writer (io/writer (.getOutputStream sock))]
        (with-open [reader (io/reader (.getInputStream sock))]
          (doto writer (.write "foo\r\n") .flush)
          (is (= "oof" (.readLine reader)))
          (doto writer (.write "bar\r\n") .flush)
          (is (= "rab" (.readLine reader)))
          (doto writer (.write "foo") (.write "bar\r\n") .flush)
          (is (= "raboof" (.readLine reader))))))))
