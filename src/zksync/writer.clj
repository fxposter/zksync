(ns zksync.writer
  (:require [clojure.set :as set]
            [zksync.reader :as r]
            [clojure.tools.logging :as log])
  (:import (org.apache.curator.framework.api BackgroundCallback CuratorEvent PathAndBytesable Pathable)
           (org.apache.curator.framework CuratorFramework)))

(declare ^BackgroundCallback default-callback
         ^BackgroundCallback created-callback
         ^BackgroundCallback children-updated-callback
         log-event)

(defrecord ZooKeeper [^CuratorFramework client]
  r/Listener
  (updated [_ path value]
    (.forPath ^PathAndBytesable (.inBackground (.setData client) default-callback) path value))
  (created [this path value]
    (.forPath ^PathAndBytesable (.inBackground (.creatingParentsIfNeeded (.create client)) created-callback {:client this, :value value}) path value))
  (deleted [_ path]
    (.forPath ^Pathable (.inBackground (.deletingChildrenIfNeeded (.delete client)) default-callback) path))
  (children-updated [this path children]
    (.forPath ^Pathable (.inBackground (.getChildren client) children-updated-callback {:client this, :children children}) path)))

(def ^:private ^BackgroundCallback default-callback
  (reify BackgroundCallback
    (processResult [_ _client event]
      (log-event event))))

(def ^:private ^BackgroundCallback created-callback
  (reify BackgroundCallback
    (processResult [_ _client event]
      (log-event event :success-result-codes #{0 -110})
      (when (= -110 (.getResultCode event))
        (let [context (.getContext event)]
          (r/updated (:client context) (.getPath event) (:value context)))))))

(def ^:private ^BackgroundCallback children-updated-callback
  (reify BackgroundCallback
    (processResult [_ _client event]
      (log-event event :success-result-codes #{0 -101})
      (when (not= -101 (.getResultCode event))
        (let [existing-children (set (.getChildren event))
              context (.getContext event)
              children (:children context)
              client (:client context)
              children-to-delete (set/difference existing-children children)]
          (doseq [child children-to-delete]
            (r/deleted client (str (.getPath event) "/" child))))))))

(defn- log-event [^CuratorEvent event & {:keys [success-result-codes]
                                         :or   {success-result-codes #{0}}}]
  (let [result-code (.getResultCode event)]
    (if (success-result-codes result-code)
      (log/debug event)
      (log/error event))))
