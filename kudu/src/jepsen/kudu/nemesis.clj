(ns jepsen.kudu.nemesis
  "Nemesis for Apache Kudu."
  (:refer-clojure :exclude [test])
  (:use clojure.pprint)
  (:require [jepsen
             [client :as client]
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
