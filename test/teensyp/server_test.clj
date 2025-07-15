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

(defn ->buffer [s]
  (buf/str->buffer s StandardCharsets/US_ASCII))

(defn- hello-handler
  ([write]
   (write (->buffer "hello\n"))
   (write tcp/CLOSE))
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
    (String. b StandardCharsets/US_ASCII)))

(defn- echo-handler
  ([_write])
  ([_state buffer write]
   (let [s (<-buffer buffer)]
     (write (->buffer s))))
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
     (-> (apply str (reverse line)) (str "\r\n") ->buffer write)))
  ([_state _ex]))

(deftest server-readline-test
  (with-open [_ (tcp/start-server
                 {:port 3460
                  :handler reverse-line-handler})]
    (with-open [sock (Socket. "localhost" 3460)]
      (with-open [writer (io/writer (.getOutputStream sock))]
        (with-open [reader (io/reader (.getInputStream sock))]
          (doto writer (.write "foo\r\n") .flush)
          (is (= "oof" (.readLine reader)))
          (doto writer (.write "bar\r\n") .flush)
          (is (= "rab" (.readLine reader)))
          (doto writer (.write "foo") (.write "bar\r\n") .flush)
          (is (= "raboof" (.readLine reader))))))))

(deftest server-ordering-test
  (let [messages (promise)]
    (with-open [_ (tcp/start-server
                   {:port 3461
                    :handler
                    (fn
                      ([_] [])
                      ([state buf write]
                       (Thread/sleep 20)
                       (write (->buffer "ack"))
                       (conj state (<-buffer buf)))
                      ([state _]
                       (deliver messages (conj state :close))))})]
      (with-open [sock (Socket. "localhost" 3461)]
        (.setSoLinger sock true 0)  ; abrupt disconnect
        (with-open [writer (io/writer (.getOutputStream sock))]
          (doto writer (.write "foo") .flush)
          (Thread/sleep 10)
          (doto writer (.write "bar") .flush)
          (Thread/sleep 30)))
      (is (= ["foo" "bar" :close] (deref messages 100 :timeout))))))

(deftest server-pause-and-resume-test
  (let [read-count (atom 0)
        write-ref  (atom nil)]
    (with-open [_ (tcp/start-server
                   {:port 3462
                    :handler
                    (fn
                      ([write] (reset! write-ref write))
                      ([_ _ _] (swap! read-count inc))
                      ([_ _]))})]
      (with-open [sock (Socket. "localhost" 3462)]
        (with-open [writer (io/writer (.getOutputStream sock))]
          (future (doto writer (.write "foo") .flush))
          (Thread/sleep 10)
          (@write-ref tcp/PAUSE-READS)
          (Thread/sleep 10)
          (future (doto writer (.write "bar") .flush))
          (Thread/sleep 10)
          (is (= 1 @read-count))
          (@write-ref tcp/RESUME-READS)
          (Thread/sleep 20)
          (is (= 2 @read-count)))))))

(deftest server-write-callback-test
  (with-open [_ (tcp/start-server
                 {:port 3463
                  :handler
                  (fn
                    ([write]
                     (write (->buffer "foo")
                            (fn []
                              (write (->buffer "bar")
                                     (fn []
                                       (write (->buffer "\r\n")))))))
                    ([_ _ _])
                    ([_ _]))})]
    (with-open [sock (Socket. "localhost" 3463)]
      (with-open [reader (io/reader (.getInputStream sock))]
        (is (= "foobar" (.readLine reader)))))))
