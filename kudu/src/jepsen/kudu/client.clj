(ns jepsen.kudu.client
  "Thin wrappers around Kudu's java client."
  (:require [clojure.tools.logging :refer :all]
            [clojure.pprint :refer [pprint]])
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
                                   RowResultIterator]))

(defn sync-client
  "Builds and returns a new synchronous Kudu client."
  [master-addresses]
  (let [builder (new KuduClient$KuduClientBuilder master-addresses)
        client (. builder build)]
    client))

(defn close-client
  [sync-client]
  (try (.close sync-client) (catch Exception e (info "Error closing client: " e))))

(defn column-schema
  ([name type] (.build (.key (new ColumnSchema$ColumnSchemaBuilder name, type) false)))
  ([name type key?] (.build (.key (new ColumnSchema$ColumnSchemaBuilder name, type) key?))))

(defn create-table
  [sync-client name schema options]
  (.createTable sync-client name schema options))

(defn open-table
  [sync-client name]
  (.openTable sync-client name))

(defn rr->tuple
  "Transforms a RowResult into a tuple."
  [row-result]
  (let [columns (.getColumns (.getSchema row-result))]
    (into {}
          (for [[idx column] (map-indexed vector columns)]
            (let [name (.getName column)
                  value (case (.ordinal (.getType column))
                          ;; Clojure transforms enums in literals
                          ;; so we have to use ordinals :(
                          0 (.getByte row-result idx)
                          1 (.getShort row-result idx)
                          2 (.getInt row-result idx)
                          3 (.getLong row-result idx)
                          4 (.getBinaryCopy row-result idx)
                          5 (.getString row-result idx)
                          6 (.getBoolean row-result idx)
                          7 (.getFloat row-result idx)
                          8 (.getDouble row-result idx)
                          9 (.getLong row-result idx))]
              {(keyword name) value})))))

(defn drain-scanner->tuples
  "Drains a scanner to a vector of tuples."
  [scanner]
  (let [result (atom [])]
    (while (.hasMoreRows scanner)
      (do (let [rr-iter (.nextRows scanner)]
            (while (.hasNext rr-iter)
              (do (swap! result conj (rr->tuple (.next rr-iter))))))))
    @result))
