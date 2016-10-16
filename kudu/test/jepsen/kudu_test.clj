(ns jepsen.kudu-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.tests :as tests]
            [jepsen.kudu :as kudu]))

(deftest kudu-test
  (is (:valid? (:results (jepsen/run! (kudu/kudu-test))))))
