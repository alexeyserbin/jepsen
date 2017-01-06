(ns jepsen.kudu
  "Tests for Apache Kudu"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen
             [control :as c :refer [|]]
             [db :as db]
             [net :as net]
             [tests :as tests]
             [util :as util :refer [meh]]]
            [jepsen.control.net :as cnet :refer [heal]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.kudu.nemesis :as kn]
            [jepsen.kudu.util :as ku]))

(defn db
  []
  "The setup/teardown procedures for an Apache Kudu DB node.  The node can
  runs either a master or a tablet server."
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "Setting up environment")

        ;; Restore the network.  This is to clean-up left-overs from prior
        ;; nemesis-induced grudges.
        (meh (cnet/heal))

        (c/exec :service :rsyslog :start)

        (ku/prepare-node test node)
        (ku/sync-time test node)
        (ku/start-kudu test node)

        (info node "Kudu ready")))

    (teardown! [_ test node]
      (c/su
        (info node "Tearing down Kudu")
        (when (.contains (:tservers test) node)
          (ku/stop-kudu-tserver test node))
        (when (.contains (:masters test) node)
          (ku/stop-kudu-master test node))
        ;; TODO collect log-files and collect table data, for debugging.
        (info node "Kudu stopped")))))


(defn merge-options
  "Merges the common options for all Kudu tests with the specific options
  set on the test itself. This does not include 'db' or 'nodes'."
  [opts]
  (let [default-opts {:os         debian/os
                      :net        net/iptables
                      :db         (db)
                      ;; The list of nodes that will run tablet servers.
                      :tservers   [:n1 :n2 :n3 :n4 :n5]
                      ;; The list of nodes that will run the kudu master.
                      :masters    [:m1]
                      :table-name
                        (str (:name opts) "-" (System/currentTimeMillis))
                      :ts-hb-interval-ms 1000
                      :ts-hb-max-failures-before-backoff 3
                      :ts-raft-hb-interval-ms 50
                      :ranges      []}

        custom-opts (merge default-opts opts)

        derived-opts {:master-addresses
                      (ku/concatenate-addresses ku/master-rpc-port
                                                (:masters custom-opts))
                      :nodes (into [] (concat (:tservers custom-opts)
                                              (:masters custom-opts)))}]
    (merge custom-opts derived-opts)))

;; Common setup for all kudu tests.
(defn kudu-test
  "Sets up the test parameters."
  [opts]
  (merge-options opts))
