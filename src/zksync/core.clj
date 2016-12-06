(ns zksync.core
  (:require [zookeeper :as zk]
            [zookeeper.internal :as zi]
            [clojure.set :as set])
  (:import (org.apache.zookeeper ZooKeeper)))

(defprotocol ZooKeeperWriterProtocol
  (ensure-exists [_ path])
  (ensure-not-exists [_ path])
  (ensure-data [_ path data])
  (ensure-children [_ path children]))

(defrecord ZooKeeperWriter [client]
  ZooKeeperWriterProtocol
  (ensure-exists [_ path]
    (zk/create-all client path :persistent? true))
  (ensure-not-exists [_ path]
    (zk/delete-all client path))
  (ensure-data [_ path data]
    (zk/set-data client path data -1))
  (ensure-children [_ path children]
    (let [existing-children (zk/children client path)
          children-to-create (set/difference (set children) (set existing-children))
          children-to-delete (set/difference (set existing-children) (set children))]
      (doseq [child children-to-create]
        (zk/create client (str (if (= "/" path) "" path) "/" child) :persistent? true))
      (doseq [child children-to-delete]
        (zk/delete-all client (str (if (= "/" path) "" path) "/" child))))))

(defprotocol ZooKeeperSyncerProtocol
  (exists [_ path args])
  (children [_ path args])
  (data [_ path args]))

(defrecord ZooKeeperSyncer [c writer]
  ZooKeeperSyncerProtocol
  (exists [_ path args]
    (let [exists (apply zk/exists c path args)]
      (if exists
        (ensure-exists writer path)
        (ensure-not-exists writer path))
      exists))
  (children [_ path args]
    (let [children (apply zk/children c path args)]
      (ensure-children writer path children)
      children))
  (data [_ path args]
    (let [data (apply zk/data c path args)]
      (ensure-data writer path (:data data))
      data)))

(declare watch)

(defn children-watcher [s]
  (fn [e]
    (when (= :NodeChildrenChanged (:event-type e))
      (let [fetched-children (children s (:path e) [:watcher (children-watcher s)])]
        (doseq [child fetched-children]
          (watch s (str (if (= "/" (:path e)) "" (:path e)) "/" child)))))))

(defn data-watcher [s]
  (fn [e]
    (when (= :NodeDataChanged (:event-type e))
      (data s (:path e) [:watcher (data-watcher s)]))))

(defn exists-watcher [s]
  (fn [e]
    (when (#{:NodeCreated :NodeDeleted} (:event-type e))
      (exists s (:path e) [:watcher (exists-watcher s)])
      (when (= :NodeCreated (:event-type e))
        (let [fetched-children (children s (:path e) [:watcher (children-watcher s)])]
          (doseq [child fetched-children]
            (watch s (str (if (= "/" (:path e)) "" (:path e)) "/" child))))
        (data s (:path e) [:watcher (data-watcher s)])))))

(defn watch
  ([s path]
   (watch s path false))
  ([s path root]
   (when (exists s path (if root [:watcher (exists-watcher s)] []))
     (when-let [children (children s path [:watcher (children-watcher s)])]
       (doseq [child children]
         (watch s (str (if (= "/" path) "" path) "/" child))))
     (data s path [:watcher (data-watcher s)]))))

(defn start [source destination paths]
  (let [source-box (volatile! nil)
        destination-box (volatile! nil)]
    (letfn [(watcher [e]
              (when (= (:keeper-state e) :Disconnected)
                (if-let [source-conn @source-box] (zk/close source-conn))
                (if-let [destination-conn @destination-box] (zk/close destination-conn))
                (do-sync)))
            (do-sync []
              (let [source-conn (vreset! source-box (ZooKeeper. source 5000 (zi/make-watcher watcher)))
                    destination-conn (vreset! destination-box (ZooKeeper. destination 5000 (zi/make-watcher watcher)))
                    writer (ZooKeeperWriter. destination-conn)
                    syncer (ZooKeeperSyncer. source-conn writer)]
                (doseq [path paths]
                  (watch syncer path true))))]
      (do-sync)
      [source-box destination-box])))

(defn stop [syncer]
  (doseq [box syncer]
    (if-let [conn @box] (zk/close conn))))

(defn running? [syncer]
  (every? #(= :CONNECTED (if-let [conn @%] (zk/state conn))) syncer))