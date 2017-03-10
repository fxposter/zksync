(ns zksync.reader
  (:import (org.apache.curator.framework CuratorFramework)
           (org.apache.curator.framework.recipes.cache TreeCacheListener TreeCache)
           (java.util Collections)
           (org.apache.zookeeper KeeperException$NoNodeException)))

(declare convert-listener
         add-tree-listener
         children-commands)

(defprotocol Listener
  (updated [_ path value])
  (created [_ path value])
  (deleted [_ path])
  (children-updated [_ path children]))

(defn add-listeners [client path listeners]
  (let [tree-cache (.build (TreeCache/newBuilder client path))]
    (add-tree-listener tree-cache
                       (fn [e]
                         (case (:type e)
                           :NODE_ADDED (doseq [listener listeners]
                                         (created listener (:path e) (:data e)))
                           :NODE_REMOVED (doseq [listener listeners]
                                           (deleted listener (:path e)))
                           :NODE_UPDATED (doseq [listener listeners]
                                           (updated listener (:path e) (:data e)))
                           :INITIALIZED (doseq [[path children] (children-commands tree-cache path)]
                                          (doseq [listener listeners]
                                            (children-updated listener path children)))
                           nil)))
    tree-cache))

(defn start-listener [^TreeCache listener]
  (.start listener))

(defn stop-listener [^TreeCache listener]
  (.close listener))

(defn- add-tree-listener [^TreeCache tree-cache f]
  (.addListener
    (.getListenable tree-cache)
    (convert-listener f)))

(defn- convert-listener [f]
  (reify TreeCacheListener
    (childEvent [this client event]
      (let [e (let [data (.getData event)]
                (if data
                  {:type (keyword (str (.getType event)))
                   :path (.getPath (.getData event))
                   :data (.getData (.getData event))}
                  {:type (keyword (str (.getType event)))}))]
        (f e)))))

(defn- children-for [^TreeCache tc path]
  (set (.keySet (or (.getCurrentChildren tc path) (Collections/emptyMap)))))

(defn- children-commands [tc path]
  (let [children (children-for tc path)]
    (concat [[path children]]
            (mapcat #(children-commands tc (str path "/" %)) children))))
