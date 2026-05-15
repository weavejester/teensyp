(ns teensyp.stress-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [teensyp.server :as tcp]
            [teensyp.buffer :as buf]
            [teensyp.stream :as stream])
  (:import [java.io BufferedReader InputStream OutputStream]
           [java.net Socket]
           [java.nio.charset StandardCharsets]))

(def ascii
  StandardCharsets/US_ASCII)

(defn- double-handler
  ([_])
  ([_ buffer write]
   (loop []
     (when-some [s (buf/read-line buffer ascii)]
       (let [x (Integer/parseInt s)]
         (write (buf/str->buffer (str (* 2 x) "\n") ascii))
         (recur)))))
  ([_ _]))

(defn- stream-double-handler [^InputStream in ^OutputStream out]
  (with-open [r ^BufferedReader (io/reader in)
              w (io/writer out)]
    (loop []
      (when-some [s (.readLine r)]
        (let [x (Integer/parseInt s)]
          (.write w (str (* 2 x) "\n"))
          (.flush w)
          (recur))))))

(defn- server-output-sum [handler input ^long port ^long timeout]
  (let [output-sum (atom 0)]
    (with-open [_ (tcp/start-server
                   {:port port
                    :handler handler
                    :write-queue-size 256})]
      (with-open [sock (Socket. "localhost" port)]
        (let [write-thread
              (Thread.
               #(let [w (io/writer (.getOutputStream sock))]
                  (doseq [i input]
                    (.write w (str i "\n"))
                    (.flush w))))
              read-thread 
              (Thread.
               #(let [r ^BufferedReader (io/reader (.getInputStream sock))]
                  (dotimes [_ (count input)]
                    (let [x (Integer/parseInt (.readLine r))]
                      (swap! output-sum + x)))))]
          (.start write-thread)
          (.start read-thread)
          (.join read-thread timeout)
          (.join write-thread timeout)
          @output-sum)))))

(deftest input-output-stress-test
  (let [amount  16384
        threads 16
        numbers (partition (/ amount threads) (shuffle (range amount)))
        sum     (/ (* (dec amount) amount) 2)
        handler double-handler
        results (map-indexed
                 (fn [i xs]
                   (future (server-output-sum handler xs (+ 4567 i) 5000)))
                 numbers)]
    (is (= (* 2 sum)
           (time (reduce + (map deref results)))))))

(deftest streaming-stress-test
  (let [amount  4096
        threads 8
        numbers (partition (/ amount threads) (shuffle (range amount)))
        sum     (/ (* (dec amount) amount) 2)
        handler (stream/stream-handler stream-double-handler)
        results (map-indexed
                 (fn [i xs]
                   (future (server-output-sum handler xs (+ 5678 i) 5000)))
                 numbers)]
    (is (= (* 2 sum)
           (time (reduce + (map deref results)))))))
