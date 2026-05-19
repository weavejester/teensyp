(ns teensyp.stream-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [teensyp.buffer :as buf]
            [teensyp.server :as tcp]
            [teensyp.stream :as stream])
  (:import [java.io BufferedReader InputStream OutputStream]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(defn- ->bytes ^bytes [^String s]
  (.getBytes s StandardCharsets/US_ASCII))

(defn- <-buffer ^String [b]
  (buf/buffer->str b StandardCharsets/US_ASCII))

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
        socket   (reify tcp/Socket
                   (queue-write [_ buf callback]
                      (let [x (if (instance? ByteBuffer buf) (<-buffer buf) buf)]
                        (swap! output conj x)
                        (callback)))
                   (socket-info [_] {}))
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

(deftest stream-backpressure-test
  (let [in-stream (promise)
        handler   (stream/stream-handler
                    (fn [^InputStream in ^OutputStream _out]
                      (deliver in-stream in))
                    {:read-buffer-size 4})
        output    (atom [])
        socket    (reify tcp/Socket
                    (queue-write [_ buf callback]
                      (swap! output conj buf)
                      (when callback (callback)))
                    (socket-info [_] {}))
        buf       (ByteBuffer/allocate 4)
        state     (handler socket)]
    (doto buf (.put (->bytes "abc")) .flip)
    (handler state socket buf)
    (is (= [] @output))
    (doto buf .compact (.put (byte \d)) .flip)
    (handler state socket buf)
    (is (= [::tcp/pause-reads] @output))
    (.read ^InputStream @in-stream (byte-array 4) 0 4)
    (is (= [::tcp/pause-reads ::tcp/resume-reads] @output))))
