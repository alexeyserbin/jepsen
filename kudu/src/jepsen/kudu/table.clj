(ns jepsen.kudu.table
  "Utilities to work with kudu tables, for testing."
  (:require [clojure.tools.logging :refer :all]
            [clojure.pprint :refer [pprint]]
            [jepsen.kudu.client :as c])
  (:import [org.apache.kudu ColumnSchema
                            ColumnSchema$ColumnSchemaBuilder
                            Schema
                            Type])
  (:import [org.apache.kudu.client AbstractKuduScannerBuilder
                                   AsyncKuduScanner$ReadMode
                                   BaseKuduTest
                                   CreateTableOptions
                                   KuduClient
                                   KuduClient$KuduClientBuilder
                                   KuduPredicate
                                   KuduPredicate$ComparisonOp
                                   KuduScanner
                                   KuduSession
                                   KuduTable
                                   OperationResponse
                                   PartialRow
                                   RowResult
                                   RowResultIterator
                                   Upsert]))
;;
;; KV Table utilities
;;

;; Creates a basic schema for a Key/Value table where the key is a string and
;; the value is an int.
(def kv-table-schema
  (new Schema [(c/column-schema "key" Type/STRING true)
               (c/column-schema "value" Type/INT32 false)]))

;; Returns options to create a K/V table with partitions on 'ranges'
;; Ranges should be a vector of [start, end) keys. The resulting
;; table will have (count ranges) tablets with the exact coverage
;; set on the ranges.
(defn kv-table-options
  [ranges num-replicas]
  (let [options (new CreateTableOptions)]
    (.setRangePartitionColumns options ["key"])
    (.setNumReplicas options num-replicas)
    (doseq [range ranges]
      (let [lower (.newPartialRow kv-table-schema)
            upper (.newPartialRow kv-table-schema)]
        (.addString lower "key" (get range 0))
        (.addString upper "key" (get range 1))
        (.addRangePartition options lower upper)))
    options))


;; Upsert a row on a KV table
(defn kv-write
  [sync-client table key value]
  (let [upsert (.newUpsert table)
        row (.getRow upsert)]
    (.addString row "key" key)
    (.addInt row "value" (int value))
    (let [response (.apply (.newSession sync-client) upsert)]
      (assert (not (.hasRowError response)) (str "Got a row error: " response)))))

;; Read the value associated with key.
(defn kv-read
  [sync-client table key]
  (let [scanner-builder (.newScannerBuilder sync-client table)
        predicate (KuduPredicate/newComparisonPredicate (c/column-schema "key" Type/STRING)
                                                        KuduPredicate$ComparisonOp/EQUAL
                                                        key)]
    (.readMode scanner-builder AsyncKuduScanner$ReadMode/READ_AT_SNAPSHOT)
    (.addPredicate scanner-builder predicate)
    (let [rows (c/drain-scanner->tuples (.build scanner-builder))]
      (case (count rows)
        0 nil
        1 (:value (get rows 0))
        (assert false (str "Expected 0 or 1 rows. Got: " (count rows)))))))
