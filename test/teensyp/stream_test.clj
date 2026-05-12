(ns teensyp.stream-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [teensyp.buffer :as buf]
            [teensyp.server :as tcp]
            [teensyp.stream :as stream])
  (:import [java.io InputStream OutputStream]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.util.concurrent Executors]))

(defn- ->bytes [^String s]
  (.getBytes s StandardCharsets/US_ASCII))

(defn- <-buffer [b]
  (buf/buffer->str b StandardCharsets/US_ASCII))

(deftest test-stream-handler
  (let [executor (Executors/newFixedThreadPool 2)
        error    (promise)
        handler  (stream/stream-handler
                  executor
                  (fn [^InputStream in ^OutputStream out]
                    (try
                      (with-open [r (io/reader in)
                                  w (io/writer out)]
                        (.write w (str "foo" (.readLine r)))
                        (.flush w)
                        (let [x (.readLine r)]
                          (.write w (str "bar" x)))
                        (.flush w))
                      (catch Exception ex
                        (deliver error ex))))
                  {})
        buffer   (ByteBuffer/allocate 128)
        output   (atom [])
        write    #(let [x (if (instance? ByteBuffer %) (<-buffer %) %)]
                    (swap! output conj x))
        state   (handler write)]
    (.put buffer (->bytes "Hello\nWor"))
    (.flip buffer)
    (handler state buffer write)
    (Thread/sleep 100)
    (.compact buffer)
    (.put buffer (->bytes "ld\n"))
    (.flip buffer)
    (handler state buffer write)
    (Thread/sleep 100)
    (is (= ["fooHello" "barWorld" tcp/CLOSE tcp/CLOSE] @output))
    (is (not (realized? error)))))
