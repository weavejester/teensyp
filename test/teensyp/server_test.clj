(ns teensyp.server-test
  (:require [clojure.test :refer [deftest is]]
            [teensyp.server :as tcp]))

(deftest server-test
  (let [stop-server (tcp/start-server {:port 3456})]
    (is (fn? stop-server))
    (stop-server)))
