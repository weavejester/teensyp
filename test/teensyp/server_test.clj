(ns teensyp.server-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [teensyp.server :as tcp])
  (:import [java.net Socket]
           [java.nio ByteBuffer]))

(deftest server-close-test
  (with-open [server (tcp/start-server {:port 3456})]
    (is (instance? java.io.Closeable server))))

(defn- hello-handler [_ write]
  (write (ByteBuffer/wrap (.getBytes "hello\n")))
  (write nil))

(deftest server-read-test
  (with-open [_ (tcp/start-server {:port 3457 :handler hello-handler})]
    (let [sock (Socket. "localhost" 3457)]
      (with-open [reader (io/reader (.getInputStream sock))]
        (is (= "hello" (.readLine reader)))))))
