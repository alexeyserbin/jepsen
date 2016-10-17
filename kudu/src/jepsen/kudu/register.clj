(ns jepsen.kudu.register
  "Simple linearizability test for a read/write register."
  (:refer-clojure :exclude [test])
  (:require [jepsen [kudu :as kudu]
             [client :as client]
             [util :refer [meh]]
             [checker    :as checker]
             [generator  :as gen]
             [nemesis    :as nemesis]]
            [jepsen.kudu.client :as kc]
            [jepsen.kudu.table :as kt]
            [jepsen.kudu.nemesis :as kn]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]))

(def register-key "x")

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 10)})

(defn client
  [table-created? kclient ktable]
  (reify client/Client
    (setup! [_ test _]
      ;; Create the client and create/open the table.
      (let [kclient (kc/sync-client (:master-addresses test))
            ktable (locking table-created?
                     (if (compare-and-set! table-created? false true)
                       (kc/create-table kclient
                                        (:table-name test)
                                        kt/kv-table-schema
                                        (kt/kv-table-options [] (:num_replicas test)))
                       (kc/open-table kclient (:table-name test))))]
        (client table-created? kclient ktable)))

    (invoke! [_ _ op]
      (case (:f op)
        :read  (assoc op :type :ok,
                         :value (kt/kv-read kclient ktable register-key))
        :write (do (kt/kv-write kclient ktable register-key (:value op))
                   (assoc op :type :ok))))

    (teardown! [_ _]
      (kc/close-client kclient))))

(defn register-test
  [opts]
  (kudu/kudu-test
    (merge
      {:name    "rw-register"
       :client (client (atom false) nil nil)
       :concurrency 10
       :num_replicas 5 ;; (count (:tservers test))
       ;; :num_replicas 1
       ;; :nemesis  nemesis/noop
       :nemesis (kn/partition-random-halves)
       :model   (model/register)
       :generator (->> (gen/reserve 5 (gen/mix [w r]) r)
                       (gen/delay 1/2)
                       (gen/nemesis
                         (gen/seq (cycle [(gen/sleep 2)
                                          {:type :info, :f :start}
                                          (gen/sleep 5)
                                          {:type :info, :f :stop}])))
                       (gen/time-limit 60))
       :checker (checker/compose
                  {:perf   (checker/perf)
                   :linear checker/linearizable})}
      opts)))
