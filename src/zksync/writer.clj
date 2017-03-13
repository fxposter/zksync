(ns zksync.writer
  (:require [clojure.set :as set]
            [zksync.reader :as r]
            [clojure.tools.logging :as log])
  (:import (org.apache.curator.framework.api BackgroundCallback CuratorEvent PathAndBytesable Pathable)
           (org.apache.curator.framework CuratorFramework)
           (org.apache.curator.framework.state ConnectionStateListener ConnectionState)))

(declare ^BackgroundCallback default-callback
         ^BackgroundCallback created-callback
         ^BackgroundCallback initialized-callback
         log-event)

(deftype ZooKeeper [^CuratorFramework client]
  r/Listener
  (updated [_ path value]
    (.forPath ^PathAndBytesable (.inBackground (.setData client) default-callback) path value))
  (created [this path value]
    (.forPath ^PathAndBytesable (.inBackground (.creatingParentsIfNeeded (.create client)) created-callback {:client this, :value value}) path value))
  (deleted [_ path]
    (.forPath ^Pathable (.inBackground (.deletingChildrenIfNeeded (.delete client)) default-callback) path))
  (initialized [this path f]
    (.forPath ^Pathable (.inBackground (.getChildren client) initialized-callback {:client this, :fn f}) path))
  (request-reinitialization-fn [_ path f]
    (.addListener
      (.getConnectionStateListenable client)
      (reify ConnectionStateListener
        (stateChanged [_ _ newState]
          (if (.isConnected newState)
            (f)))))))


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

(def ^:private ^BackgroundCallback initialized-callback
  (reify BackgroundCallback
    (processResult [_ _client event]
      (log-event event :success-result-codes #{0 -101})

      (let [context (.getContext event)
            node ((:fn context))
            client (:client context)]
        (if node
          (if (not= -101 (.getResultCode event))
            (let [value (:value node)
                  children (:children node)
                  existing-children (set (.getChildren event))
                  children-to-delete (set/difference existing-children children)]
              (r/updated client (.getPath event) value)
              (doseq [child children-to-delete]
                (r/deleted client (str (.getPath event) "/" child))))
            (r/created client (.getPath event) (:value node)))
          (r/deleted client (.getPath event)))))))

(defn- log-event [^CuratorEvent event & {:keys [success-result-codes]
                                         :or   {success-result-codes #{0}}}]
  (let [result-code (.getResultCode event)]
    (if (success-result-codes result-code)
      (log/debug event)
      (log/error event))))
