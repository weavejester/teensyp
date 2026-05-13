(ns teensyp.concurrent
  "Utility functions and macros for concurrent programming."
  (:import [java.util.concurrent.locks Lock]))

(defmacro with-lock
  "Lock the supplied lock before running the body, and finally unlock
  the lock once the body is complete or has thrown an exception."
  [lock & body]
  `(let [^Lock lock# ~lock]
     (.lock lock#)
     (try ~@body (finally (.unlock lock#)))))
