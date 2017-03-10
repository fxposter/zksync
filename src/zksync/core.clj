(ns zksync.core
  (:require [clojure.set :as set])
  (:import [java.util.concurrent LinkedBlockingQueue]
           [org.apache.curator.framework.recipes.cache TreeCache TreeCacheListener]
           [org.apache.curator.framework CuratorFrameworkFactory CuratorFramework]
           [org.apache.curator.retry ExponentialBackoffRetry RetryNTimes]
           [org.apache.zookeeper KeeperException$NodeExistsException KeeperException$NoNodeException]
           (java.util Map Collections)
           (org.apache.curator.framework.api UnhandledErrorListener BackgroundCallback)))

(defn- add-listener [^TreeCache tree-cache f]
  (.addListener
    (.getListenable tree-cache)
    (reify TreeCacheListener
      (childEvent [this client event]
        (let [e (let [data (.getData event)]
                  (if data
                    {:type (keyword (str (.getType event)))
                     :path (.getPath (.getData event))
                     :data (.getData (.getData event))}
                    {:type (keyword (str (.getType event)))}))]
          (f e))))))

(defn- children-for [^TreeCache tc path]
  (try
    (set (.keySet (or (.getCurrentChildren tc path) (Collections/emptyMap))))
    (catch KeeperException$NoNodeException e
      #{})))

(defn- children-commands [tc path]
  (let [children (children-for tc path)]
    (concat [[:children path children
              (mapcat #(children-commands tc (str path "/" %)) children)]])))

(def ^:private zkset-background-callback
  (reify BackgroundCallback
    (processResult [_ client event])))
      ;(prn event))))

(defn- zkset [^CuratorFramework client path value]
  (.forPath (.inBackground (.setData client) ^BackgroundCallback zkset-background-callback) path value))

(def ^:private zkcreate-background-callback
  (reify BackgroundCallback
    (processResult [_ client event]
      ;(prn event)
      (when (= -110 (.getResultCode event))
        (zkset client (.getPath event) (.getContext event))))))

(defn- zkcreate [^CuratorFramework client path value]
  (.forPath (.inBackground (.creatingParentsIfNeeded (.create client)) ^BackgroundCallback zkcreate-background-callback value) path value))

(defn- zkdelete [^CuratorFramework client path]
  (.forPath (.inBackground (.deletingChildrenIfNeeded (.delete client)) ^BackgroundCallback zkset-background-callback) path))

(def ^:private zkchildren-background-callback
  (reify BackgroundCallback
    (processResult [_ client event]
      ;(prn event)
      (when (not= -101 (.getResultCode event))
        (let [existing-children (set (.getChildren event))
              children (.getContext event)
              children-to-delete (set/difference existing-children children)]
          (doseq [child children-to-delete]
            (zkdelete client (str (.getPath event) "/" child))))))))

(defn zkchildren [^CuratorFramework client path value]
  (.forPath (.inBackground (.getChildren client) ^BackgroundCallback zkchildren-background-callback value) path))

(defn no-retry []
  (RetryNTimes. 0 0))

(defn exponential-backoff-retry [sleep-time retries]
  (ExponentialBackoffRetry. sleep-time retries))

(defn curator-framework [connect-string retry-policy & {:keys [namespace]}]
  (let [builder (CuratorFrameworkFactory/builder)]
    (.connectString builder connect-string)
    (.retryPolicy builder retry-policy)
    (.namespace builder namespace)
    (.build builder)))

(defn start-source [client path queues]
  (let [tree-cache (.build (TreeCache/newBuilder client path))]
    (add-listener tree-cache (fn [e]
                               (case (:type e)
                                 :NODE_ADDED (doseq [^LinkedBlockingQueue queue queues]
                                               (.put queue [:add (:path e) (:data e)]))
                                 :NODE_REMOVED (doseq [^LinkedBlockingQueue queue queues]
                                                 (.put queue [:remove (:path e)]))
                                 :NODE_UPDATED (doseq [^LinkedBlockingQueue queue queues]
                                                 (.put queue [:data (:path e) (:data e)]))
                                 :INITIALIZED (doseq [command (children-commands tree-cache path)]
                                                (doseq [^LinkedBlockingQueue queue queues]
                                                  (.put queue command)))
                                 nil)))
    (.start tree-cache)
    tree-cache))

(defn sources [client paths queues]
  (mapv #(start-source client % queues) paths))

(defn- sink-worker [client ^LinkedBlockingQueue queue]
  (let [[command path value] (.take queue)]
    (let [action (case command
                   :add (zkcreate client path value)
                   :remove (zkdelete client path)
                   :data (zkset client path value)
                   :children (zkchildren client path value)
                   ::stop ::stop
                   nil)]
      (when (not= action ::stop)
        (recur client queue)))))

(defn start-sink [client queue]
  (let [th (Thread. #(sink-worker client queue))]
    (.start th)
    th))

(defn create [source-client paths sink-clients]
  (let [sinks-and-queues (mapv (fn [c] [c (LinkedBlockingQueue.)]) sink-clients)
        sink-threads (mapv (fn [[sink queue]] (start-sink sink queue)) sinks-and-queues)
        sink-queues (mapv (fn [[sink queue]] queue) sinks-and-queues)
        source-tree-caches (sources source-client paths sink-queues)]
    {:source-client      source-client
     :source-tree-caches source-tree-caches
     :sink-clients       sink-clients
     :sink-queues        sink-queues
     :sink-threads       sink-threads}))

(defn start [sync]
  (.start ^CuratorFramework (:source-client sync))
  (doseq [^CuratorFramework client (:sink-clients sync)]
    (.start client))
  sync)

(defn stop [sync]
  (doseq [^TreeCache client (:source-tree-caches sync)]
    (.close client))
  (.close ^CuratorFramework (:source-client sync))
  (doseq [^CuratorFramework client (:sink-clients sync)]
    (.close client))
  (doseq [^LinkedBlockingQueue queue (:sink-queues sync)]
    (.clear queue)
    (.put queue [::stop]))
  (doseq [^Thread thread (:sink-threads sync)]
    (.join thread))
  sync)


;(def sync (create (curator-framework "127.0.0.1:2181" (no-retry)) ["/root"] [(curator-framework "127.0.0.1:2181" (no-retry) :namespace "writer")]))
;(start sync)
;(stop sync)
