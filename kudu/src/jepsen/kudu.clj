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
             [util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.kudu.nemesis :as kn]))

;; TODO make it possible to set the version via options
(def kudu-version "1.2.0")

;; TODO allow to replace the binaries with locally built ones
(def kudu-repo-url
  (str "http://repos.jenkins.cloudera.com/kudu" kudu-version
       "-nightly/debian/jessie/amd64/kudu"))
(def kudu-pkg-os-release (str "jessie-kudu" kudu-version))
(def kudu-repo-name "kudu-nightly")
(def kudu-repo-apt-line
  (str "deb " kudu-repo-url " " kudu-pkg-os-release " contrib"))
(def kudu-required-packages [:kudu-master :kudu-tserver :ntp])

(defn concatenate-addresses
  "Returns a list of the Kudu master addresses, given a list of node names."
  [hosts]
  (str/join "," (map #(str (name %) ":7051") hosts)))

(defn ntp-in-sync?
  "If the NTP server is in sync state?"
  []
  (try (c/exec :ntp-wait :-n1 :-s1)
       true
       (catch RuntimeException _ false)))

(defn kudu-master-in-service?
  "If the Kudu master server provides its services already?"
  [node]
  (try (c/exec :kudu :table :list node)
       true
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
   "enable stats"
   "statistics loopstats peerstats clockstats sysstats"
   "filegen loopstats file loopstats type day enable"
   "filegen peerstats file peerstats type day enable"
   "filegen clockstats file clockstats type day enable"
   "filegen sysstats file sysstats type day enable"
   "logconfig =syncall +clockall +sysall +peerall"
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
        (info node "Setting up environment")

        (c/exec :service :rsyslog :start)

        (let [repo-file (str "/etc/apt/sources.list.d/"
                             (name kudu-repo-name) ".list")]
          (when-not (cu/exists? repo-file)
            (info "Adding " kudu-repo-name " package repositoy")
            (debian/add-repo! kudu-repo-name kudu-repo-apt-line)
            (info "Fetching " kudu-repo-name " package key")
            (c/exec :curl :-fLSs (str kudu-repo-url "/" "archive.key") |
                    :apt-key :add :-)))

        (when-not (debian/installed? kudu-required-packages)
          (info node "Installing Kudu")
          (debian/update!)

          ;; Install tserver, master and ntp in all nodes.
          (debian/install kudu-required-packages)

          ;; Install the masters flag file in all the servers.
          (c/exec :echo (str (slurp (io/resource "kudu.flags"))
                             "\n"
                             (kudu-cfg-master test))
                  :> "/etc/kudu/conf/master.gflagfile")

          ;; Install the tservers flag file in all servers.
          (c/exec :echo (str (slurp (io/resource "kudu.flags"))
                             "\n"
                             (kudu-cfg-tserver test))
                  :> "/etc/kudu/conf/tserver.gflagfile"))

        ;; When ntpd is not in synchronized state, revamp its configs
        ;; and restart the ntpd.
        (when-not (ntp-in-sync?)
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
          (c/exec :service :kudu-master :restart)
          ;; Wait for master services avaiable (awaiting for catalog manager)
          (loop [iteration 0]
            (when-not (kudu-master-in-service? node)
              (if (> iteration 10)
                (c/exec :false)
                (do
                  (c/exec :sleep :1)
                  (recur (inc iteration))))))
          (c/exec :kudu :master :status node))

        (when (.contains (:tservers test) node)
          (info node "Starting Kudu Tablet Server")
          (c/exec :service :kudu-tserver :restart)
          (c/exec :kudu :tserver :status node))


        (info node "Kudu ready")))

    (teardown! [_ test node]
      (info node "Tearing down Kudu")
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
                      :ranges      []}

        custom-opts (merge default-opts opts)

        derived-opts {:master-addresses
                        (concatenate-addresses (:masters custom-opts))
                      :nodes
                        (into [] (concat (:tservers custom-opts)
                                         (:masters custom-opts)))}]
    (merge custom-opts derived-opts)))

;; Common setup for all kudu tests.
(defn kudu-test
  "Sets up the test parameters."
  [opts]
  (merge-options opts))
