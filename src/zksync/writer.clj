(ns zksync.writer
  (:require [clojure.set :as set]
            [zksync.reader :as r])
  (:import (org.apache.curator.framework.api BackgroundCallback)
           (org.apache.curator.framework CuratorFramework)))

(declare ^BackgroundCallback default-callback
         ^BackgroundCallback create-callback
         ^BackgroundCallback keep-children-callback)

(defrecord ZooKeeper [^CuratorFramework client]
  r/Listener
  (set-value [_ path value]
    (.forPath (.inBackground (.setData client) default-callback) path value))
  (create [_ path value]
    (.forPath (.inBackground (.creatingParentsIfNeeded (.create client)) create-callback value) path value))
  (delete [_ path]
    (.forPath (.inBackground (.deletingChildrenIfNeeded (.delete client)) default-callback) path))
  (keep-children [_ path children]
    (.forPath (.inBackground (.getChildren client) keep-children-callback children) path)))

(def ^:private ^BackgroundCallback default-callback
  (reify BackgroundCallback
    (processResult [_ client event])))
    ;(prn event))))

(def ^:private ^BackgroundCallback create-callback
  (reify BackgroundCallback
    (processResult [_ client event]
      ;(prn event)
      (when (= -110 (.getResultCode event))
        (r/set-value client (.getPath event) (.getContext event))))))

(def ^:private ^BackgroundCallback keep-children-callback
  (reify BackgroundCallback
    (processResult [_ client event]
      ;(prn event)
      (when (not= -101 (.getResultCode event))
        (let [existing-children (set (.getChildren event))
              children (.getContext event)
              children-to-delete (set/difference existing-children children)]
          (doseq [child children-to-delete]
            (r/delete client (str (.getPath event) "/" child))))))))
