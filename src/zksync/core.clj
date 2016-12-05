(ns zksync.core
  (:require [zookeeper :as zk]
            [clojure.set :as set]))

(defprotocol ZooKeeperWriterProtocol
  (ensure-exists [_ path])
  (ensure-not-exists [_ path])
  (ensure-data [_ path data])
  (ensure-children [_ path children]))

(defrecord ZooKeeperWriter [clients]
  ZooKeeperWriterProtocol
  (ensure-exists [_ path]
    (doseq [c clients]
      (zk/create c path :persistent? true)))
  (ensure-not-exists [_ path]
    (doseq [c clients]
      (zk/delete-all c path)))
  (ensure-data [_ path data]
    (doseq [c clients]
      (zk/set-data c path data -1)))
  (ensure-children [_ path children]
    (doseq [c clients]
      (let [existing-children (zk/children c path)
            children-to-create (set/difference (set children) (set existing-children))
            children-to-delete (set/difference (set existing-children) (set children))]
        (doseq [child children-to-create]
          (zk/create c (str (if (= "/" path) "" path) "/" child) :persistent? true))
        (doseq [child children-to-delete]
          (zk/delete-all c (str (if (= "/" path) "" path) "/" child)))))))

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

(defn children-watcher [c]
  (fn [e]
    (when (= :NodeChildrenChanged (:event-type e))
      (let [fetched-children (children c (:path e) [:watcher (children-watcher c)])]
        (doseq [child fetched-children]
          (watch c (str (if (= "/" (:path e)) "" (:path e)) "/" child)))))))

(defn data-watcher [c]
  (fn [e]
    (when (= :NodeDataChanged (:event-type e))
      (data c (:path e) [:watcher (data-watcher c)]))))

(defn exists-watcher [c]
  (fn [e]
    (when (#{:NodeCreated :NodeDeleted} (:event-type e))
      (exists c (:path e) [:watcher (exists-watcher c)])
      (when (= :NodeCreated (:event-type e))
        (children c (:path e) [:watcher (children-watcher c)])
        (data c (:path e) [:watcher (data-watcher c)])))))

(defn watch
  ([r path]
   (watch r path false))
  ([r path root]
    (when (exists r path (if root [:watcher (exists-watcher r)] []))
      (when-let [children (children r path [:watcher (children-watcher r)])]
        (doseq [child children]
          (watch r (str (if (= "/" path) "" path) "/" child))))
      (data r path [:watcher (data-watcher r)]))))

(defn sync-zookeepers [source destinations paths]
  (let [source-conn (zk/connect source)
        destinations-conns (map zk/connect destinations)
        writer (ZooKeeperWriter. destinations-conns)
        syncer (ZooKeeperSyncer. source-conn writer)]
    (doseq [path paths]
      (watch syncer path true))
    (fn []
      (zk/close source-conn)
      (doseq [c destinations-conns]
        (zk/close c)))))
