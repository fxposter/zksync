(ns zksync.writer
  (:require [clojure.set :as set]
            [zksync.reader :as r]
            [clojure.tools.logging :as log])
  (:import (org.apache.curator.framework.api BackgroundCallback CuratorEvent)
           (org.apache.curator.framework CuratorFramework)))

(declare ^BackgroundCallback default-callback
         ^BackgroundCallback create-callback
         ^BackgroundCallback keep-children-callback
         log-event)

(defrecord ZooKeeper [^CuratorFramework client]
  r/Listener
  (set-value [_ path value]
    (.forPath (.inBackground (.setData client) default-callback) path value))
  (create [this path value]
    (.forPath (.inBackground (.creatingParentsIfNeeded (.create client)) create-callback {:client this, :value value}) path value))
  (delete [_ path]
    (.forPath (.inBackground (.deletingChildrenIfNeeded (.delete client)) default-callback) path))
  (keep-children [this path children]
    (.forPath (.inBackground (.getChildren client) keep-children-callback {:client this, :children children}) path)))

(def ^:private ^BackgroundCallback default-callback
  (reify BackgroundCallback
    (processResult [_ _client event]
      (log-event event))))

(def ^:private ^BackgroundCallback create-callback
  (reify BackgroundCallback
    (processResult [_ _client event]
      (log-event event :success-result-codes #{0 -110})
      (when (= -110 (.getResultCode event))
        (let [context (.getContext event)]
          (r/set-value (:client context) (.getPath event) (:value context)))))))

(def ^:private ^BackgroundCallback keep-children-callback
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
            (r/delete client (str (.getPath event) "/" child))))))))

(defn- log-event [^CuratorEvent event & {:keys [success-result-codes]
                                         :or   {success-result-codes #{0}}}]
  (let [result-code (.getResultCode event)]
    (if (success-result-codes result-code)
      (log/debug event)
      (log/error event))))
