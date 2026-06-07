(defproject dev.weavejester/teensyp "0.6.0"
  :description "A small Clojure TCP server"
  :url "https://github.com/weavejester/teensyp"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.5"]]
  :plugins [[lein-codox "0.10.8"]]
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :global-vars {*warn-on-reflection* true}
  :codox {:output-path "codox"
          :metadata {:doc/format :markdown}})
