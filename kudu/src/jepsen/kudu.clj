(ns jepsen.kudu
  "Tests for Apache Kudu"
  (:require [clojure.tools.logging :refer :all]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen
             [core :as jepsen]
             [db :as db]
             [os :as os]
             [tests :as tests]
             [control :as c :refer [|]]
             [store :as store]
             [nemesis :as nemesis]
             [generator :as gen]
             [independent :as independent]
             [reconnect :as rc]
             [util :as util :refer [meh]]]
            [jepsen.os.debian   :as debian]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]))

(def kudu-repo-name "kudu-nightly")
(def kudu-repo-apt-line "deb http://repos.jenkins.cloudera.com/kudu-nightly/debian/jessie/amd64/kudu jessie-kudu1.1.0 contrib")

(defn master-addresses
  "Returns a list of the Kudu master addresses, given a list of node names."
  [names]
  (str/join "," (map #(str (name %) ":7051") names)))

(defn kudu-cfg-master
  [test]
  (def flags ["--fs_wal_dir=/var/lib/kudu/master"
              "--fs_data_dirs=/var/lib/kudu/master"])

  ;; Only set the master addresses when there is more than one master
  (when (> (count (:masters test)) 1)
    (conj flags (str "--master_addresses=" (master-addresses (:masters test)))))

  (str/join "\n" flags))


(defn kudu-cfg-tserver
  [test]
  (str/join "\n"
            [(str "--tserver_master_addresses=" (master-addresses (:masters test)))
             "--fs_wal_dir=/var/lib/kudu/tserver"
             "--fs_data_dirs=/var/lib/kudu/tserver"]))

(defn db
  "Apache Kudu."
  []
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "Setting up environment.")
        (debian/add-repo! kudu-repo-name kudu-repo-apt-line)
        (debian/update!)
        (info node "installing Kudu")

        ;; Install both tserver and master in all nodes.
        (debian/install ["kudu-tserver" "kudu-master"])

        ;; Install the masters flag file in all the servers.
        (c/exec :echo (str (slurp (io/resource "kudu.flags"))
                           "\n"
                           (kudu-cfg-master test))
                :> "/etc/kudu/conf/master.gflagfile")

        ;; Install the tservers flag file in all servers.
        (c/exec :echo (str (slurp (io/resource "kudu.flags"))
                           "\n"
                           (kudu-cfg-tserver test))
                :> "/etc/kudu/conf/tserver.gflagfile")

        (c/exec :service :ntp :restart)

        (when (.contains (:masters test) node)
            (info node "Starting Kudu Master")
            (c/exec :service :kudu-master :restart))

        (when (.contains (:nodes test) node)
            (info node "Starting Kudu Tablet Server")
            (c/exec :service :kudu-tserver :restart))

        (info node "Kudu ready")))

    (teardown! [_ test node]
      (info node "tearing down Kudu")

      (when (.contains (:masters test) node)
          (info node "Stopping Kudu Master")
          (c/exec :service :kudu-master :stop))

      (when (.contains (:nodes test) node)
          (info node "Stopping Kudu Tablet Server")
          (c/exec :service :kudu-tserver :stop))

      (info node "Kudu stopped"))))

(defn kudu-test
  []
  (assoc tests/noop-test
    :os debian/os
    :db (db)
    ;; The list of masters to use for the tests.
    ;; Must be a vector of nodes
    :masters [:n1]))
