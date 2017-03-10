(ns zksync.curator
  (:import (org.apache.curator.retry RetryNTimes ExponentialBackoffRetry)
           (org.apache.curator.framework CuratorFrameworkFactory CuratorFramework)))

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

(defn start [^CuratorFramework client]
  (.start client))

(defn stop [^CuratorFramework client]
  (.close client))
