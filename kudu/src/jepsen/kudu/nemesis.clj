(ns jepsen.kudu.nemesis
  "Nemesis for Apache Kudu."
  (:refer-clojure :exclude [test])
  (:use clojure.pprint)
  (:require [jepsen
             [client :as client]
             [control :as c]
             [nemesis :as nm]
             [net :as net]
             [util :as util]]
            [clojure.tools.logging :refer :all]
            [jepsen.kudu.util :as ku]))


(defn tserver-partitioner
  "Tablet server partitioner: cut network links between tablet servers
  in respond to :start operation: cut network links as defined by
  (grudge nodes), and restore them back in respond in :stop operation."
  [grudge]
  (reify client/Client
    (setup! [this test _]
      (net/heal! (:net test) test)
      this)

    (invoke! [this test op]
      (case (:f op)
        :start (let [grudge (grudge (:tservers test))]
                 (nm/partition! test grudge)
                 (assoc op :value (str "Cut off " (pr-str grudge))))
        :stop  (do (net/heal! (:net test) test)
                   (assoc op :value "fully connected"))))

    (teardown! [this test]
      (net/heal! (:net test) test))))


(defn tserver-start-stopper
  "Takes a targeting function which, given a list of nodes, returns a single
  node or collection of nodes to affect, and two functions `(start! test node)`
  invoked on nemesis start, and `(stop! test node)` invoked on nemesis stop.
  Returns a nemesis which responds to :start and :stop by running the start!
  and stop! fns on each of the given nodes. During `start!` and `stop!`, binds
  the `jepsen.control` session to the given node, so you can just call `(c/exec
  ...)`.

  Re-selects a fresh node (or nodes) for each start--if targeter returns nil,
  skips the start. The return values from the start and stop fns will become
  the :values of the returned :info operations from the nemesis, e.g.:

      {:value {:n1 [:killed \"java\"]}}"
  [targeter start! stop!]
  (let [nodes (atom nil)]
    (reify client/Client
      (setup! [this test _] this)

      (invoke! [this test op]
        (locking nodes
          (assoc op :type :info, :value
                 (case (:f op)
                   :start (if-let [ns (-> test :tservers targeter util/coll)]
                            (if (compare-and-set! nodes nil ns)
                              (c/on-many ns (start! test c/*host*))
                              (str "nemesis already disrupting "
                                   (pr-str @nodes)))
                            :no-target)
                   :stop (if-let [ns @nodes]
                           (let [value (c/on-many ns (stop! test c/*host*))]
                             (reset! nodes nil)
                             value)
                           :not-started)))))

      (teardown! [this test]))))


(defn tserver-partition-random-halves
  "Cuts the tablet servers' network into randomly chosen halves."
  []
  (tserver-partitioner (comp nm/complete-grudge nm/bisect shuffle)))


(defn tserver-partition-majorities-ring
  "A grudge in which every tablet server can see a majority, but no server sees
  the *same* majority as any other."
  []
  (tserver-partitioner nm/majorities-ring))


(defn kill-restart-tserver
  "Responds to `{:f :start}` by sending SIGKILL to the tablet server on a given
  node, and when `{:f :stop}` arrives, re-starts the specified tablet server.
  Picks the node(s) using `(targeter list-of-nodes)`.  Targeter may return
  either a single node or a collection of nodes."
  ([targeter]
   (tserver-start-stopper targeter
                          (fn start [t n]
                            (c/su (c/exec :killall :-s :SIGKILL :kudu-tserver))
                            ["Killed kudu-tserver"])
                          (fn stop [t n]
                            (ku/start-kudu-tserver t n)
                            ["Started kudu-tserver"]))))

(defn tserver-hammer-time
  "Responds to `{:f :start}` by pausing the tablet server name on a given node
  using SIGSTOP, and when `{:f :stop}` arrives, resumes it with SIGCONT.
  Picks the node(s) to pause using `(targeter list-of-nodes)`, which defaults
  to `rand-nth`.  Targeter may return either a single node or a collection
  of nodes."
  ([process] (tserver-hammer-time rand-nth process))
  ([targeter process]
   (tserver-start-stopper targeter
                       (fn start [t n]
                         (c/su (c/exec :killall :-s "STOP" :kudu-tserver))
                         [:paused process])
                       (fn stop [t n]
                         (c/su (c/exec :killall :-s "CONT" :kudu-tserver))
                         [:resumed process]))))
