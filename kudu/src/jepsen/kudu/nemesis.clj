(ns jepsen.kudu.nemesis
  "Nemesis for Apache Kudu."
  (:refer-clojure :exclude [test])
  (:use clojure.pprint)
  (:require [jepsen
             [client :as client]
             [control :as c]
             [nemesis :as nm]]
            [clojure.tools.logging :refer :all]))

;; TODO replace this with some function that selects nodes based on name?
(defn replace-nodes
  [nodes]
  (let [tservers [:n1 :n2 :n3 :n4 :n5]]
    tservers))

(defn partition-random-halves
  "Cuts the network into randomly chosen halves."
  []
  (nm/partitioner (comp nm/complete-grudge nm/bisect shuffle replace-nodes)))

(defn partition-majorities-ring
  "A grudge in which every node can see a majority, but no node sees the *same*
  majority as any other."
  []
  (nm/partitioner (comp nm/majorities-ring replace-nodes)))

(defn kill-restart-service
  "Responds to `{:f :start}` by sending the given process name on a given node
  SIGKILL, and when `{:f :stop}` arrives, re-starts the specified service.
  Picks the node(s) using `(targeter list-of-nodes)`.  Targeter may return
  either a single node or a collection of nodes."
  ([targeter process service]
   (nm/node-start-stopper targeter
                         (fn start [t n]
                           (c/su (c/exec :killall :-s :SIGKILL process))
                           ["killed" process])
                         (fn stop [t n]
                           (c/su (c/exec :service service :start))
                           ["started" service "service"])))
  ;; This is for the case when process and service names are the same.
  ([targeter service] (let [process service]
                        (kill-restart-service targeter process service))))
