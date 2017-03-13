(ns zksync.reader
  (:import (org.apache.curator.framework.recipes.cache TreeCacheListener TreeCache)
           (java.util Collections)))

(declare convert-listener
         add-tree-listener
         initialize)

(defprotocol Listener
  (updated [_ path value])
  (created [_ path value])
  (deleted [_ path])
  (initialized [_ path f])
  (request-reinitialization-fn [_ path fn]))

(defn add-listeners [client path listeners]
  (let [tree-cache (.build (TreeCache/newBuilder client path))
        was-initialized (volatile! false)]
    (add-tree-listener tree-cache
                       (fn [e]
                         (case (:type e)
                           :NODE_ADDED (doseq [listener listeners]
                                         (created listener (:path e) (:data e)))
                           :NODE_REMOVED (doseq [listener listeners]
                                           (deleted listener (:path e)))
                           :NODE_UPDATED (doseq [listener listeners]
                                           (updated listener (:path e) (:data e)))
                           :INITIALIZED (do
                                          (initialize tree-cache path listeners)
                                          (if-not @was-initialized
                                            (doseq [listener listeners]
                                              (request-reinitialization-fn listener path
                                                                           #(initialize tree-cache path [listener]))))
                                          (vreset! was-initialized true))
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

(defn- build-node [^TreeCache tree-cache path]
  (let [data (.getCurrentData tree-cache path)
        children (.getCurrentChildren tree-cache path)]
    (if (and data children)
      {:value    (.getData data)
       :children (.keySet children)}
      nil)))

(defn- initialize [^TreeCache tree-cache path listeners]
  (let [children (.keySet (or (.getCurrentChildren tree-cache path) (Collections/emptyMap)))]
    (doseq [listener listeners]
      (initialized listener path #(build-node tree-cache path)))
    (doseq [child children]
      (initialize tree-cache (str path "/" child) listeners))))
