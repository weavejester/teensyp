(ns teensyp.stress-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [teensyp.server :as tcp]
            [teensyp.buffer :as buf]
            [teensyp.stream :as stream])
  (:import [java.io BufferedReader InputStream OutputStream]
           [java.net Socket]
           [java.nio.charset StandardCharsets]))

(def ascii StandardCharsets/US_ASCII)

(defn- double-handler
  ([_])
  ([_ socket buffer]
   (try (loop []
          (when-some [s (buf/read-line buffer ascii)]
            (let [x (Integer/parseInt s)]
              (tcp/write socket (buf/str->buffer (str (* 2 x) "\n") ascii))
              (recur))))
        (catch Exception ex
          (prn ex))))
  ([_ ex]
   (when ex (prn ex))))

(defn- stream-double-handler [^InputStream in ^OutputStream out]
  (with-open [r ^BufferedReader (io/reader in)
              w (io/writer out)]
    (try (loop []
           (when-some [s (.readLine r)]
             (let [x (Integer/parseInt s)]
               (.write w (str (* 2 x) "\n"))
               (.flush w)
               (recur))))
         (catch Exception ex
           (prn ex)))))

(defn- server-output-sum [input ^long port ^long timeout]
  (let [output-sum (atom 0)]
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
        @output-sum))))

(deftest input-output-stress-test
  (with-open [_ (tcp/start-server
                 {:port 4567
                  :handler double-handler
                  :write-queue-size 512})]
    (let [amount  16384
          threads 16
          numbers (partition (/ amount threads) (shuffle (range amount)))
          sum     (/ (* (dec amount) amount) 2)
          results (map #(future (server-output-sum % 4567 5000)) numbers)]
      (is (= (* 2 sum)
             (time (reduce + (map deref results))))))))

(deftest streaming-stress-test
  (with-open [_ (tcp/start-server
                 {:port 4568
                  :handler (stream/stream-handler stream-double-handler)})]
    (let [amount  4096
          threads 8
          numbers (partition (/ amount threads) (shuffle (range amount)))
          sum     (/ (* (dec amount) amount) 2)
          results (map #(future (server-output-sum % 4568 5000)) numbers)]
      (is (= (* 2 sum)
             (time (reduce + (map deref results))))))))
