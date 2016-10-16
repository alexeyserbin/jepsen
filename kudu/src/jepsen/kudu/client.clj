(ns jepsen.kudu.client
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io    :as io]
            [clojure.string     :as str])
  (:import [org.apache.kudu.client BaseKuduTest
                                   KuduClient$KuduClientBuilder]))

(defn sync-client
  "Builds and returns a new synchronous Kudu client."
  [master-addresses]
  (def builder (new KuduClient$KuduClientBuilder master-addresses))
  (def client (. builder build))
  client)

(defn create-table
  "Creates and returns a new KuduTable with the schema:
   (key int32, column1_i int32, column2_s int32, column3_s string)

   The table will be range partitioned on the key. The ranges are:
   [  0,  50)
   [ 50, 100)
   [200, 300)

   This fails with an exception if the table already exists."
  [name sync-client]
  (def options (BaseKuduTest/getBasicTableOptionsWithNonCoveredRange))
  (def schema (BaseKuduTest/getBasicSchema))
  (def table (.createTable sync-client name schema options))
  table)



