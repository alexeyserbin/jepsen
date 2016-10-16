(defproject kudu "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Apache Kudu"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.3-SNAPSHOT"]
                 [org.apache.kudu/kudu-client "1.1.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.kudu/kudu-client "1.1.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-api] :classifier "tests"]])

; shit happens
