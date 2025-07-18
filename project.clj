(defproject dev.weavejester/teensyp "0.2.3"
  :description "A small Clojure TCP server"
  :url "https://github.com/weavejester/teensyp"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.1"]]
  :plugins [[lein-codox "0.10.8"]]
  :codox {:output-path "codox"}
  :repl-options {:init-ns teensyp.core})
