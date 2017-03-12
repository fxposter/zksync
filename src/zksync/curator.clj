(ns zksync.curator
  (:import (org.apache.curator.retry RetryNTimes ExponentialBackoffRetry)
           (org.apache.curator.framework CuratorFrameworkFactory CuratorFramework)))

(defn no-retry []
  (RetryNTimes. 0 0))

(defn exponential-backoff-retry [sleep-time retries]
  (ExponentialBackoffRetry. sleep-time retries))

(defn curator-framework [connect-string retry-policy & {:keys [namespace can-be-read-only]
                                                        :or   {can-be-read-only false}}]
  (let [builder (CuratorFrameworkFactory/builder)]
    (.connectString builder connect-string)
    (.retryPolicy builder retry-policy)
    (.namespace builder namespace)
    (.canBeReadOnly builder can-be-read-only)
    (.build builder)))

(defn start [^CuratorFramework client]
  (.start client))

(defn stop [^CuratorFramework client]
  (.close client))
