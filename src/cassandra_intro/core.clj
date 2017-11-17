(ns cassandra-intro.core
  (:require [qbits.alia :as alia]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html

(def cluster (alia/cluster {:contact-points ["localhost"]}))
(def session (alia/connect cluster))

(defn create-keyspace []
  (alia/execute session "CREATE KEYSPACE uber
                        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};"))

(defn create-table []
  (alia/execute session "CREATE TABLE uber.trips (
                        sourceid int,
                        dstid int,
                        hod int,
                        mean_travel_time float,
                        standard_deviation_travel_time float,
                        geometric_mean_travel_time float,
                        geometric_standard_deviation_travel_time float,

                        PRIMARY KEY (sourceid, mean_travel_time, dst_id));"))

(defn query-str [vals]
  (str "INSERT INTO uber.trips
        (sourceid,dstid,hod,mean_travel_time,standard_deviation_travel_time,geometric_mean_travel_time,geometric_standard_deviation_travel_time)
        VALUES ("
       (str/join ", " vals)
       ");"))

(defn read-csv []
  (println "reading csv")
  (with-open [reader (io/reader (io/resource "test.csv"))]
    (->> (drop (line-seq reader) 2)
         (map query-str)
         (run! (partial alia/execute session)))))

(defn -main []
  (create-keyspace)
  (create-table)
  (read-csv)
  (alia/shutdown session)
  (alia/shutdown cluster))
