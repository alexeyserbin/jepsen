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

;; TODO make it possible to choose between nightly builds and archived ones
;; (def kudu-repo-name "kudu-nightly")
;; (def kudu-repo-apt-line "deb http://repos.jenkins.cloudera.com/kudu1.1.0-nightly/debian/jessie/amd64/kudu jessie-kudu1.1.0 contrib")

;; TODO allow to replace the binaries with locally built ones
(def kudu-repo-url "http://repos.jenkins.cloudera.com/kudu-nightly/debian/jessie/amd64/kudu")
(def kudu-pkg-release "jessie-kudu1.2.0")
(def kudu-repo-name "kudu-nightly")
(def kudu-repo-apt-line (str "deb" " " kudu-repo-url " " kudu-pkg-release " " "contrib"))
;;(def kudu-repo-apt-line "deb http://repos.jenkins.cloudera.com/kudu-nightly/debian/jessie/amd64/kudu jessie-kudu1.2.0 contrib")

(defn concatenate-addresses
  "Returns a list of the Kudu master addresses, given a list of node names."
  [hosts]
  (str/join "," (map #(str (name %) ":7051") hosts)))

(defn ntp-in-sync?
  "If the NTP server is in sync state?"
  []
  (try ((c/exec :ntp-wait :-n1 :-s1) true)
       (catch RuntimeException _ false)))

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

(def ntpd-driftfile "/var/lib/ntp/ntp.drift")

(def ntp-common-opts
  ["tinker panic 0"
   "enable kernel"
   "enable ntp"
   "enable pps"
   "statistics loopstats peerstats clockstats sysstats"
   "filegen loopstats file loopstats type day enable"
   "filegen peerstats file peerstats type day enable"
   "filegen clockstats file clockstats type day enable"
   "filegen sysstats file sysstats type day enable"
   "logconfig =syncall +eventsall +clockall"
   "logfile /var/log/ntpd.log"
   "statsdir /var/log/ntpstats/"
   (str "driftfile " ntpd-driftfile)])

(def ntp-server-opts
  ["enable calibrate"
   "server 127.127.1.0 burst iburst minpoll 4 maxpoll 4"
   "fudge 127.127.1.0 stratum 10"])


(defn ntp-server-config
  []
  (str/join "\n" (into [] (concat ntp-common-opts ntp-server-opts))))

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
        (c/exec :curl :-fLSs (str kudu-repo-url "/" "archive.key") |
                :apt-key :add :-)
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

        ;; When ntpd is not in synchronized state, revamp its configs
        ;; and restart the ntpd.
        (when (not (ntp-in-sync?))
          (c/exec :service :ntp :stop "||" :true)
          (c/exec :echo "NTPD_OPTS='-g -N'" :> "/etc/default/ntp")
          (when (.contains (:masters test) node)
            (c/exec :echo (ntp-server-config) :> "/etc/ntp.conf"))
          (when (.contains (:tservers test) node)
            (c/exec :echo (ntp-slave-config (:masters test)):> "/etc/ntp.conf"))
          (c/exec :service :ntp :start)
          ;; Wait for 5 minutes max for ntpd to get into synchronized state.
          (c/exec :ntp-wait :-s1 :-n300))

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
