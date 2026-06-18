(ns teensyp.stream-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [teensyp.buffer :as buf]
            [teensyp.server :as tcp]
            [teensyp.stream :as stream])
  (:import [java.io BufferedReader IOException InputStream OutputStream]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.util.concurrent.locks ReentrantLock]))

(defn- ->bytes ^bytes [^String s]
  (.getBytes s StandardCharsets/US_ASCII))

(defn- <-buffer ^String [b]
  (buf/buffer->str b StandardCharsets/US_ASCII))

(defn- fake-socket
  ([writef] (fake-socket writef (fn [_ _])))
  ([writef controlf]
   (let [lock (ReentrantLock.)]
     (reify tcp/Socket
       (try-write [_ _] false)
       (queue-write [_ buf callback] (writef buf callback))
       (queue-control [_ buf callback] (controlf buf callback))
       (socket-info [_] {})
       (socket-lock [_] lock)))))

(deftest socket->output-stream-test
  (testing "output sent to socket"
    (let [output (atom [])
          socket (fake-socket 
                  (fn [buf callback]
                    (future
                      (let [x (if (instance? ByteBuffer buf) (<-buffer buf) buf)]
                        (swap! output conj x)
                        (when callback (callback))))))
          stream (stream/socket->output-stream socket)]
      (is (= [] @output))
      (.write stream (->bytes "foo"))
      (is (= ["foo"] @output))
      (.write stream (->bytes "bar"))
      (is (= ["foo" "bar"] @output))
      (.close stream)
      (is (= ["foo" "bar" ::tcp/close] @output))
      (is (thrown? IOException (.write stream (->bytes "baz"))))))
  (testing "custom close function"
    (let [output (atom [])
          socket (fake-socket
                   (fn [buf]
                     (let [x (if (instance? ByteBuffer buf) (<-buffer buf) buf)]
                       (swap! output conj x))))
          stream (stream/socket->output-stream socket {:on-close (fn [_])})]
      (.close stream)
      (is (= [] @output))
      (is (thrown? IOException (.write stream (->bytes "foo")))))))

(deftest input-stream-handler-test
  (testing "basic handler"
    (let [in-stream (promise)
          handler   (stream/input-stream-handler
                     (fn [^InputStream in _socket]
                       (deliver in-stream in)))
          socket    (fake-socket (fn [_ _]))
          out-buf   (ByteBuffer/allocate 16)
          in-buf    (byte-array 16)
          state     (handler socket)]
      (is (instance? InputStream (deref in-stream 1000 :timeout)))
      (testing "reads"
        (handler state socket (-> out-buf (.put (->bytes "abc")) .flip))
        (.compact out-buf)
        (is (= 3 (.read ^InputStream @in-stream in-buf)))
        (is (= "abc" (String. in-buf 0 3)))
        (handler state socket (-> out-buf (.put (->bytes "foobar")) .flip))
        (.compact out-buf)
        (is (= 6 (.read ^InputStream @in-stream in-buf 3 13)))
        (is (= "abcfoobar" (String. in-buf 0 9))))
      (testing "closing"
        (handler state nil)
        (is (= -1 (.read ^InputStream @in-stream in-buf 9 10))))))
  (testing "custom close function"
    (let [closed  (promise)
          handler (stream/input-stream-handler
                   (fn [^InputStream in _socket]
                     (.close in))
                   {:on-close
                    (fn [_socket] (deliver closed true))})
          socket  (fake-socket (fn [_]))]
      (handler socket)
      (is (true? (deref closed 1000 false))))))

(deftest input-stream-backpressure-test
  (let [in-stream (promise)
        handler   (stream/input-stream-handler
                   (fn [^InputStream in _socket]
                     (deliver in-stream in))
                   {:read-buffer-size 4})
        output    (atom [])
        socket    (fake-socket
                   (fn [_ _])
                   (fn [event callback]
                     (swap! output conj event)
                     (when callback (callback))))
        buf       (ByteBuffer/allocate 4)
        state     (handler socket)]
    (doto buf (.put (->bytes "abc")) .flip)
    (handler state socket buf)
    (is (= [] @output))
    (doto buf .compact (.put (->bytes "def")) .flip)
    (handler state socket buf)
    (is (= [::tcp/pause-reads] @output))
    (.read ^InputStream @in-stream (byte-array 4) 0 4)
    (is (= [::tcp/pause-reads ::tcp/resume-reads] @output))))

(deftest stream-handler-test
  (let [error    (promise)
        handler  (stream/stream-handler
                  (fn [^InputStream in ^OutputStream out]
                    (try
                      (with-open [r ^BufferedReader (io/reader in)
                                  w (io/writer out)]
                        (.write w (str "foo" (.readLine r)))
                        (.flush w)
                        (.write w (str "bar" (.readLine r))))
                      (catch Exception ex
                        (deliver error ex)))))
        buffer   (ByteBuffer/allocate 128)
        output   (atom [])
        socket   (fake-socket
                  (fn [buf callback]
                    (let [x (if (instance? ByteBuffer buf) (<-buffer buf) buf)]
                      (swap! output conj x)
                      (when callback (callback)))))
        state   (handler socket)]
    (.put buffer (->bytes "Hello\nWor"))
    (.flip buffer)
    (handler state socket buffer)
    (Thread/sleep 100)
    (.compact buffer)
    (.put buffer (->bytes "ld\n"))
    (.flip buffer)
    (handler state socket buffer)
    (Thread/sleep 100)
    (is (= ["fooHello" "barWorld" ::tcp/close] @output))
    (is (not (realized? error)))))

(deftest stream-close-test
  (testing "closing only input stream"
    (let [result  (promise)
          handler (stream/stream-handler
                   (fn [^InputStream in _out]
                     (.close in)
                     (deliver result (.read in (byte-array 8) 0 8))))
          output  (atom [])
          socket  (fake-socket (fn [buf callback]
                                 (swap! output conj buf)
                                 (when callback callback)))]
      (handler socket)
      (is (= -1 (deref result 1000 :timeout)))
      (is (= [] @output))))
  (testing "closing input stream while reading in separate thread"
    (let [result  (promise)
          handler (stream/stream-handler
                   (fn [^InputStream in _out]
                     (future (deliver result (.read in (byte-array 8) 0 8)))
                     (.close in)))
          output  (atom [])
          socket  (fake-socket (fn [buf callback]
                                 (swap! output conj buf)
                                 (when callback (callback))))]
      (handler socket)
      (is (= -1 (deref result 1000 :timeout)))
      (is (= [] @output))))
  (testing "closing only output stream"
    (let [error   (promise)
          handler (stream/stream-handler
                   (fn [_in ^OutputStream out]
                     (.close out)
                     (try (.write out (.getBytes "foo") 0 3)
                          (catch Exception ex (deliver error ex)))))
          output  (atom [])
          socket  (fake-socket (fn [buf callback]
                                 (swap! output conj buf)
                                 (when callback (callback))))]
      (handler socket)
      (is (instance? java.io.IOException (deref error 1000 :timeout)))
      (is (= [] @output))))
  (testing "closing both streams"
    (let [done    (promise)
          handler (stream/stream-handler
                   (fn [^InputStream in ^OutputStream out]
                     (.close in)
                     (.close out)
                     (deliver done true)))
          output  (atom [])
          socket  (fake-socket (fn [buf callback]
                                 (swap! output conj buf)
                                 (when callback (callback))))]
      (handler socket)
      (is (true? (deref done 1000 :timeout)))
      (is (= [::tcp/close] @output)))))
