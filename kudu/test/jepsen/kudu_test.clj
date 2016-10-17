(ns jepsen.kudu-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.tests :as tests]
            [jepsen.kudu :as kudu]
            [jepsen.kudu.register :as kudu-register]))

(deftest kudu-test (is (:valid? (:results (jepsen/run! (kudu-register/register-test {}))))))
