(ns teensyp.server-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [teensyp.server :as tcp]
            [teensyp.buffer :as buf])
  (:import [java.io BufferedReader]
           [java.net InetSocketAddress Socket StandardSocketOptions]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(defn- nil-handler
  ([_socket])
  ([_state _socket _buffer])
  ([_state _exception]))

(deftest server-close-test
  (testing "external close"
    (with-open [server (tcp/run-server {:port 3456, :handler nil-handler})]
      (let [sock (Socket. "localhost" 3456)]
        (is (instance? java.io.Closeable server))
        (Thread/sleep 10)
        (.close ^java.io.Closeable server)
        (Thread/sleep 10)
        (is (neg? (.read (.getInputStream sock)))))))
  (testing "close after read"
    (let [closed? (atom false)
          handler (fn
                    ([_])
                    ([_ sock _] (tcp/close sock) nil)
                    ([_ _] (reset! closed? true)))]
      (with-open [_ (tcp/run-server {:port 3556, :handler handler})]
        (let [sock (Socket. "localhost" 3556)]
          (with-open [writer (io/writer (.getOutputStream sock))]
            (doto writer (.write "foo") .flush)
            (Thread/sleep 10)
            (is @closed?)
            (is (neg? (.read (.getInputStream sock))))))))))

(defn- ->buffer [s]
  (buf/str->buffer s StandardCharsets/US_ASCII))

(defn- hello-handler
  ([socket]
   (tcp/write socket (->buffer "hello\n"))
   (tcp/close socket))
  ([_state _socket _buffer])
  ([_state _exception]))

(deftest server-write-test
  (with-open [_ (tcp/run-server
                 {:port 3457
                  :handler hello-handler})]
    (let [sock (Socket. "localhost" 3457)]
      (with-open [reader ^BufferedReader (io/reader (.getInputStream sock))]
        (is (= "hello" (.readLine reader)))))))

(defn- <-buffer [^ByteBuffer buf]
  (let [b (byte-array (.remaining buf))]
    (.get buf b)
    (String. b StandardCharsets/US_ASCII)))

(defn- echo-handler
  ([_socket])
  ([_state socket buffer]
   (let [s (<-buffer buffer)]
     (tcp/write socket (->buffer s))))
  ([_state _exception]))

(deftest server-echo-test
  (with-open [_ (tcp/run-server
                 {:port 3458
                  :handler echo-handler})]
    (let [sock (Socket. "localhost" 3458)]
      (with-open [writer (io/writer (.getOutputStream sock))]
        (with-open [reader ^BufferedReader (io/reader (.getInputStream sock))]
          (doto writer (.write "foo\n") .flush)
          (is (= "foo" (.readLine reader)))
          (doto writer (.write "bar\n") .flush)
          (is (= "bar" (.readLine reader)))
          (doto writer (.write "foo") (.write "bar\n") .flush)
          (is (= "foobar" (.readLine reader))))))))

(deftest server-socket-close-test
  (let [closed? (promise)]
    (with-open [_ (tcp/run-server
                   {:port 3459
                    :handler
                    (fn
                      ([_])
                      ([_ _ _])
                      ([_ _] (deliver closed? true)))})]
      (.close (Socket. "localhost" 3459))
      (is (deref closed? 100 false)))))

(defn- reverse-line-handler
  ([_socket])
  ([_state socket buffer]
   (when-some [line (buf/read-line buffer StandardCharsets/US_ASCII)]
     (let [rev (str (apply str (reverse line)) "\r\n")]
       (tcp/write socket (->buffer rev)))))
  ([_state _ex]))

(deftest server-readline-test
  (with-open [_ (tcp/run-server
                 {:port 3460
                  :handler reverse-line-handler})]
    (with-open [sock (Socket. "localhost" 3460)]
      (with-open [writer (io/writer (.getOutputStream sock))]
        (with-open [reader ^BufferedReader (io/reader (.getInputStream sock))]
          (doto writer (.write "foo\r\n") .flush)
          (is (= "oof" (.readLine reader)))
          (doto writer (.write "bar\r\n") .flush)
          (is (= "rab" (.readLine reader)))
          (doto writer (.write "foo") (.write "bar\r\n") .flush)
          (is (= "raboof" (.readLine reader))))))))

(deftest server-ordering-test
  (let [messages (promise)]
    (with-open [_ (tcp/run-server
                   {:port 3461
                    :handler
                    (fn
                      ([_] "")
                      ([state sock buf]
                       (Thread/sleep 20)
                       (tcp/write sock (->buffer "ack"))
                       (str state (<-buffer buf)))
                      ([state _]
                       (deliver messages (str state "//CLOSE"))))})]
      (with-open [sock (Socket. "localhost" 3461)]
        (.setSoLinger sock true 0)  ; abrupt disconnect
        (with-open [writer (io/writer (.getOutputStream sock))]
          (doto writer (.write "foo") .flush)
          (Thread/sleep 10)
          (doto writer (.write "bar") .flush)
          (Thread/sleep 30)))
      (is (= "foobar//CLOSE" (deref messages 100 :timeout))))))

(deftest server-pause-and-resume-test
  (testing "write, pause, write, resume"
    (let [read-count (atom 0)
          socket     (atom nil)]
      (with-open [_ (tcp/run-server
                     {:port 3462
                      :handler
                      (fn
                        ([sock] (reset! socket sock))
                        ([_ _ ^ByteBuffer buf]
                         (.position buf (.limit buf))  ; fake read
                         (swap! read-count inc))
                        ([_ _]))})]
        (with-open [sock (Socket. "localhost" 3462)]
          (with-open [writer (io/writer (.getOutputStream sock))]
            (doto writer (.write "foo") .flush)
            (Thread/sleep 10)
            (tcp/pause-reads @socket)
            (Thread/sleep 10)
            (doto writer (.write "bar") .flush)
            (Thread/sleep 10)
            (is (= 1 @read-count))
            (tcp/resume-reads @socket)
            (Thread/sleep 10)
            (is (= 2 @read-count)))))))
  (testing "write more than can be read, pause, resume"
    (let [read-count (atom 0)
          socket     (atom nil)]
      (with-open [_ (tcp/run-server
                     {:port 3463
                      :handler
                      (fn
                        ([sock] (reset! socket sock))
                        ([_ _ ^ByteBuffer buf]
                         (.position buf 3)  ; fake read of only 3 bytes
                         (swap! read-count inc))
                        ([_ _]))})]
        (with-open [sock (Socket. "localhost" 3463)]
          (with-open [writer (io/writer (.getOutputStream sock))]
            (doto writer (.write "foobar") .flush)
            (Thread/sleep 10)
            (tcp/pause-reads @socket)
            (Thread/sleep 10)
            (is (= 1 @read-count))
            (tcp/resume-reads @socket)
            (Thread/sleep 10)
            (is (= 2 @read-count)))))))
  (testing "write, buffer, resume"
    (let [read-count (atom 0)
          socket     (atom nil)]
      (with-open [_ (tcp/run-server
                     {:port 3462
                      :handler
                      (fn
                        ([sock] (reset! socket sock))
                        ([_ _ ^ByteBuffer buf]
                         (when (pos? ^long @read-count)
                           (.position buf (.limit buf)))  ; fake read
                         (swap! read-count inc))
                        ([_ _]))})]
        (with-open [sock (Socket. "localhost" 3462)]
          (with-open [writer (io/writer (.getOutputStream sock))]
            (doto writer (.write "foo") .flush)
            (Thread/sleep 10)
            (is (= 1 @read-count))
            (tcp/resume-reads @socket)
            (Thread/sleep 10)
            (is (= 2 @read-count))
            (tcp/resume-reads @socket)
            (Thread/sleep 10)
            (is (= 2 @read-count) "shouldn't read if read buffer empty")))))))

(deftest server-write-callback-test
  (with-open [_ (tcp/run-server
                 {:port 3464
                  :handler
                  (fn
                    ([sock]
                     (tcp/write
                      sock (->buffer "foo")
                      (fn []
                        (tcp/write
                         sock (->buffer "bar")
                         (fn [] (tcp/write sock (->buffer "\r\n")))))))
                    ([_ _ _])
                    ([_ _]))})]
    (with-open [sock (Socket. "localhost" 3464)]
      (with-open [reader ^BufferedReader (io/reader (.getInputStream sock))]
        (is (= "foobar" (.readLine reader)))))))

(deftest server-write-limit-test
  (let [exceptions (atom [])]
    (with-open [_ (tcp/run-server
                   {:port 3465
                    :write-buffer-size 4
                    :write-queue-size 2
                    :handler
                    (fn
                      ([sock]
                       (try (tcp/queue-write sock (->buffer "toobig\n") nil)
                            (catch Exception ex (swap! exceptions conj ex)))
                       (try (tcp/queue-write sock (->buffer "1\n") nil)
                            (tcp/queue-write sock (->buffer "2\n") nil)
                            (tcp/queue-write sock (->buffer "3\n") nil)
                            (catch Exception ex (swap! exceptions conj ex))))
                      ([_ _ _])
                      ([_ _]))})]
      (with-open [sock (Socket. "localhost" 3465)]
        (with-open [reader ^BufferedReader (io/reader (.getInputStream sock))]
          (is (= "1" (.readLine reader)))
          (is (= "2" (.readLine reader)))))
      (is (= [{:err ::tcp/write-queue-over-capacity}
              {:err ::tcp/write-queue-full}]
             (mapv ex-data @exceptions))))))

(deftest server-read-limit-test
  (let [input (atom [])]
    (with-open [_ (tcp/run-server
                   {:port 3460
                    :read-buffer-size 3
                    :handler (fn
                               ([_] {})
                               ([_ _ buffer]
                                (swap! input conj (<-buffer buffer))
                                (Thread/sleep 20))
                               ([_ _]))})]
      (with-open [sock (Socket. "localhost" 3460)]
        (with-open [writer (io/writer (.getOutputStream sock))]
          (doto writer (.write "foo") .flush)
          (doto writer (.write "bar") .flush)
          (Thread/sleep 10)
          (is (= ["foo"] @input))
          (Thread/sleep 50)
          (is (= ["foo" "bar"] @input)))))))

(deftest server-socket-exception-test
  (testing "Exception on init"
    (let [error (promise)]
      (with-open [_ (tcp/run-server
                     {:port 3466
                      :handler
                      (fn
                        ([_] (throw (ex-info "Testing" {})))
                        ([_ _ _])
                        ([_ ex] (deliver error ex)))})]
        (Socket. "localhost" 3466)
        (let [err (deref error 5000 :timeout)]
          (is (instance? clojure.lang.ExceptionInfo err))
          (is (= "Testing" (ex-message err)))))))
  (testing "Exception on message receive"
    (let [error (promise)]
      (with-open [_ (tcp/run-server
                     {:port 3467
                      :handler
                      (fn
                        ([_])
                        ([_ _ buf] (throw (ex-info (<-buffer buf) {})))
                        ([_ ex] (deliver error ex)))})]
        (with-open [sock (Socket. "localhost" 3467)]
          (with-open [writer (io/writer (.getOutputStream sock))]
            (.write writer "Hello World")
            (.flush writer)))
        (let [err (deref error 5000 :timeout)]
          (is (instance? clojure.lang.ExceptionInfo err))
          (is (= "Hello World" (ex-message err))))))))

(deftest server-socket-info-test
  (let [sock-info (promise)]
    (with-open [_ (tcp/run-server
                   {:port 3468
                    :handler
                    (fn
                      ([sock] (deliver sock-info (tcp/socket-info sock)))
                      ([_ _ _])
                      ([_ _]))})]
      (with-open [_ (Socket. "localhost" 3468)])
      (let [info (deref sock-info 5000 :timeout)]
        (is (= #{:local-address :remote-address} (set (keys info))))
        (is (= 3468 (.getPort ^InetSocketAddress (:local-address info))))))))

(deftest socket-option-test
  (with-open [server (tcp/server-channel (tcp/run-server
                                          {:port 3469
                                           :handler nil-handler
                                           :reuse-address? true
                                           :recv-buffer-size 2048}))]
    (is (true? (.getOption server StandardSocketOptions/SO_REUSEADDR)))
    (is (= 2048 (.getOption server StandardSocketOptions/SO_RCVBUF)))))

(deftest direct-read-buffer-test
  (testing "default is not direct"
    (let [direct-buffer? (promise)]
      (with-open [_ (tcp/run-server
                     {:port 3469
                      :handler
                      (fn
                        ([_])
                        ([_ _ ^ByteBuffer buf]
                         (deliver direct-buffer? (.isDirect buf)))
                        ([_ _]))})]
        (with-open [sock (Socket. "localhost" 3469)]
          (with-open [writer (io/writer (.getOutputStream sock))]
            (.write writer "Hello World")
            (.flush writer)))
        (is (false? (deref direct-buffer? 1000 :timeout))))))
  (testing "direct buffer can be specified"
    (let [direct-buffer? (promise)]
      (with-open [_ (tcp/run-server
                     {:port 3470
                      :direct-read-buffer? true
                      :handler
                      (fn
                        ([_])
                        ([_ _ ^ByteBuffer buf]
                         (deliver direct-buffer? (.isDirect buf)))
                        ([_ _]))})]
        (with-open [sock (Socket. "localhost" 3470)]
          (with-open [writer (io/writer (.getOutputStream sock))]
            (.write writer "Hello World")
            (.flush writer)))
        (is (true? (deref direct-buffer? 1000 :timeout)))))))

(deftest server-variadic-options-test
  (with-open [_ (tcp/run-server :port 3471 :handler echo-handler)]
    (let [sock (Socket. "localhost" 3471)]
      (with-open [writer (io/writer (.getOutputStream sock))]
        (with-open [reader ^BufferedReader (io/reader (.getInputStream sock))]
          (doto writer (.write "foo\n") .flush)
          (is (= "foo" (.readLine reader)))
          (doto writer (.write "bar\n") .flush)
          (is (= "bar" (.readLine reader)))
          (doto writer (.write "foo") (.write "bar\n") .flush)
          (is (= "foobar" (.readLine reader))))))))
