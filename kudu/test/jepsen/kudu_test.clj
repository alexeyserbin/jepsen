(ns jepsen.kudu-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.nemesis :as nemesis]
            [jepsen.tests :as tests]
            [jepsen.kudu :as kudu]
            [jepsen.kudu.nemesis :as kn]
            [jepsen.kudu.register :as kudu-register]))

(defn check
  [tcasefun opts]
  (is (:valid? (:results (jepsen/run! (tcasefun opts))))))

(defmacro dt
  [tfun tsuffix topts]
  (let [tname# (symbol (str (name tfun) "-" tsuffix))]
  `(clojure.test/deftest ~tname# (check ~tfun ~topts))))

(defn dt-func
  [tfun tsuffix topts]
  `(dt ~tfun ~tsuffix ~topts))

(defmacro instantiate-tests
  [tfun config topts]
  (let [seqtfun# (reduce (fn [out _] (conj out tfun)) [] (eval config))
        seqtsuffix# (reduce (fn [out e]
                              (conj out (:suffix e))) [] (eval config))
        seqtopts# (reduce (fn [out e]
                            (conj out (merge (eval topts)
																					   {:nemesis (:nemesis e)})))
                          [] (eval config))]
    `(do ~@(map dt-func seqtfun# seqtsuffix# seqtopts#))))

;; Specific tests and their configurations.
(def register-test kudu-register/register-test)
(def register-test-configs
  [{:suffix "random-halves"
    :nemesis '(kn/partition-random-halves)}
   {:suffix "majorities-ring"
    :nemesis '(kn/partition-majorities-ring)}
   {:suffix "kill-restart-2-tservers"
    :nemesis '(kn/kill-restart-service
                (comp (partial take 2) shuffle kn/replace-nodes)
                "kudu-tserver")}
   {:suffix "kill-restart-3-tservers"
    :nemesis '(kn/kill-restart-service
                (comp (partial take 3) shuffle kn/replace-nodes)
                "kudu-tserver")}
   {:suffix "hammer-all-tservers"
    :nemesis '(nemesis/hammer-time
                (comp shuffle kn/replace-nodes)
                "kudu-tserver")}
   {:suffix "hammer-3-tservers"
    :nemesis '(nemesis/hammer-time
                (comp (partial take 3) shuffle kn/replace-nodes)
                "kudu-tserver")}
   {:suffix "hammer-1-tserver"
    :nemesis '(nemesis/hammer-time
                (comp (partial take 1) shuffle kn/replace-nodes)
                "kudu-tserver")}])

(defmacro instantiate-all-kudu-tests
  [opts]
  `(instantiate-tests register-test register-test-configs ~opts))

(instantiate-all-kudu-tests {})
