(ns teensyp.server-test
  (:require [clojure.test :refer [deftest is]]
            [teensyp.server :as tcp]))

(deftest a-test
  (is (fn? (tcp/start-server {}))))
