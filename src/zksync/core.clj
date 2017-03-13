(ns zksync.core
  (:require [clojure.set :as set]
            [zksync.writer :as w]
            [zksync.reader :as r]
            [zksync.curator :as curator]))

(defn create [reader-client paths writer-clients]
  (let [writers (mapv w/->ZooKeeper writer-clients)
        listeners (mapv #(r/add-listeners reader-client % writers) paths)]
    {:reader-client  reader-client
     :listeners      listeners
     :writer-clients writer-clients}))

(defn start [sync]
  (doseq [client (:writer-clients sync)]
    (curator/start client))
  (curator/start (:reader-client sync))
  (doseq [listener (:listeners sync)]
    (r/start-listener listener))
  sync)

(defn stop [sync]
  (doseq [client (:listeners sync)]
    (r/stop-listener client))
  (doseq [client (:writer-clients sync)]
    (curator/stop client))
  (curator/stop (:reader-client sync))
  sync)
