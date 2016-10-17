(ns jepsen.kudu
  "Tests for Apache Kudu"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen
             [db :as db]
             [net :as net]
             [tests :as tests]
             [control :as c :refer [|]]
             [util :refer [meh]]]
            [jepsen.os.debian   :as debian]
            [jepsen.kudu.nemesis :as kn]))

;; TODO allow to replace the binaries with locally built ones
(def kudu-repo-name "kudu-nightly")
(def kudu-repo-apt-line "deb http://repos.jenkins.cloudera.com/kudu-nightly/debian/jessie/amd64/kudu jessie-kudu1.1.0 contrib")

(defn concatenate-addresses
  "Returns a list of the Kudu master addresses, given a list of node names."
  [hosts]
  (str/join "," (map #(str (name %) ":7051") hosts)))

(defn kudu-cfg-master
  [test]
  (def flags ["--fs_wal_dir=/var/lib/kudu/master"
              "--fs_data_dirs=/var/lib/kudu/master"])

  ;; Only set the master addresses when there is more than one master
  (when (> (count (:masters test)) 1)
    (conj flags (str "--master_addresses=" (concatenate-addresses (:masters test)))))

  (str/join "\n" flags))


(defn kudu-cfg-tserver
  [test]
  (str/join "\n"
            [(str "--tserver_master_addrs=" (concatenate-addresses (:masters test)))
             "--fs_wal_dir=/var/lib/kudu/tserver"
             "--fs_data_dirs=/var/lib/kudu/tserver"
             "--raft_heartbeat_interval_ms=50"]))

(def ntp-common-opts ["statistics loopstats peerstats clockstats"
                      "filegen loopstats file loopstats type day enable"
                      "filegen peerstats file peerstats type day enable"
                      "filegen clockstats file clockstats type day enable"
                      "driftfile /var/lib/ntp/ntp.drift"
                      "logconfig =syncstatus +allevents +allinfo +allstatus"
                      "logfile /var/log/ntpd.log"
                      "statsdir /var/log/ntpstats/"
                      "server 127.127.1.0" 
                      "fudge 127.127.1.0 stratum 10"])

(def ntp-server-opts [])

(defn ntp-server-config [] (str/join "\n" (into [] (concat ntp-common-opts ntp-server-opts))))
(defn ntp-slave-config
  [masters]
  (str/join "\n"
            (into [](concat ntp-common-opts
                            [(str "server "
                                  (name (first masters))
                                  " burst iburst prefer minpoll 4 maxpoll 4")]))))

(defn db
  []
  "Apache Kudu."
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "Setting up environment.")
        (debian/add-repo! kudu-repo-name kudu-repo-apt-line)
        (debian/update!)
        (info node "installing Kudu")

        ;; Install tserver, master and ntp in all nodes.
        (debian/install ["kudu-tserver" "kudu-master" "ntp"])

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

        (c/exec :service :rsyslog :start)

        (when (.contains (:masters test) node)
          (c/exec :echo (ntp-server-config) :> "/etc/ntp.conf"))

        (when (.contains (:tservers test) node)
          (c/exec :echo (ntp-slave-config (:masters test)):> "/etc/ntp.conf"))

        ;; TODO for NTP to be stable we should only start it (not restart it)
        ;; but it needs to be restarted if the config params change.
        ;; If the kudu daemons keep failing to start change this from :restart to :start.
        (c/exec :service :ntp :start)

        (when (.contains (:masters test) node)
            (info node "Starting Kudu Master")
            (c/exec :service :kudu-master :restart))

        (when (.contains (:tservers test) node)
            (info node "Starting Kudu Tablet Server")
            (c/exec :service :kudu-tserver :restart))


        (info node "Kudu ready")))

    (teardown! [_ test node]
      (info node "tearing down Kudu")
      (c/su
        (when (.contains (:masters test) node)
          (info node "Stopping Kudu Master")
          (meh (->> (c/exec :service :kudu-master :stop)))
          (meh (->> (c/exec :rm :-rf "/var/lib/kudu/master"))))

        (when (.contains (:tservers test) node)
          (info node "Stopping Kudu Tablet Server")
          (meh (->> (c/exec :service :kudu-tserver :stop)))
          (meh (->> (c/exec :rm :-rf "/var/lib/kudu/tserver")))))

      ;; TODO collect log-files and collect table data, for debugging.
      (info node "Kudu stopped"))))

;; Merges the common options for all kudu tests with the specific options set on the
;; test itself. This does not include 'db' or 'nodes'.
(defn common-options
  [opts]
  (let [common-opts       {:os                debian/os
                           :name              (str "apache-kudu-" (:name opts) "-test")
                           :net               net/iptables
                           :db                (db)
                           :client            (:client opts)
                           ;; The list of nodes that will run tablet servers.
                           :tservers          [:n1 :n2 :n3 :n4 :n5]
                           ;; The list of nodes that will run the kudu master.
                           :masters           [:m1]
                           :table-name        (str (:name opts) "-" (System/currentTimeMillis))
                           :ranges            []}
        test-opts         (dissoc opts :name :nodes :client :nemesis)
        additional-opts   {:master-addresses (concatenate-addresses (:masters common-opts))
                           :nodes            (into [] (concat (:tservers common-opts) (:masters common-opts)))
                           :nemesis          (:nemesis opts)}]
    (merge common-opts test-opts additional-opts)))

;; Common setup for all kudu tests.
(defn kudu-test
  "Sets up the test parameters."
  [opts]
  (common-options opts))
