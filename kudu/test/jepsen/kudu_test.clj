(ns jepsen.kudu-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [jepsen.core :as jepsen]
            [jepsen.nemesis :as nemesis]
            [jepsen.tests :as tests]
            [jepsen.kudu :as kudu]
            [jepsen.kudu.nemesis :as kn]
            [jepsen.kudu.register :as kudu-register]))

(defn check
  [testname nemesis]
  (is (:valid? (:results (jepsen/run! (testname {:nemesis nemesis}))))))

;; Array of nemeses to run against
(def nemeses
  [(kn/partition-majorities-ring)
   (kn/partition-random-halves)
   (nemesis/hammer-time "kudu-master")
   (nemesis/hammer-time
     (comp (partial take 3) shuffle kn/replace-nodes)
     "kudu-tserver")
   (kn/kill-restart-service
     (comp (partial take 2) shuffle kn/replace-nodes)
     "kudu-tserver" "kudu-tserver")])

; Helper macro to create deftest
(defmacro def-h
  [tname suffix base nemesis]
  `(do
     (deftest ~(symbol (str tname suffix)) (check ~base ~nemesis))))

(defmacro def-tests
  [base]
  (let [name# (-> base (name)
                  (string/replace #"^kudu-register/" "")
                  (string/replace #"-test$" ""))]
  `(do
     (def-h ~name# "-random-halves"           ~base
       (kn/partition-random-halves))

     (def-h ~name# "-majorities-ring"         ~base
       (kn/partition-majorities-ring))

     (def-h ~name# "-kill-restart-2-tservers" ~base
       (kn/kill-restart-service
         (comp (partial take 2) shuffle kn/replace-nodes) "kudu-tserver"))

     (def-h ~name# "-kill-restart-3-tservers" ~base
       (kn/kill-restart-service
         (comp (partial take 3) shuffle kn/replace-nodes) "kudu-tserver"))

     (def-h ~name# "-hammer-all-tservers"     ~base
       (nemesis/hammer-time
         (comp shuffle kn/replace-nodes) "kudu-tserver"))

     (def-h ~name# "-hammer-3-tservers"       ~base
       (nemesis/hammer-time
         (comp (partial take 3) shuffle kn/replace-nodes) "kudu-tserver"))

     (def-h ~name# "-hammer-1-tserver"         ~base
       (nemesis/hammer-time
         (comp (partial take 1) shuffle kn/replace-nodes) "kudu-tserver"))
     )))

(def-tests kudu-register/register-test)
