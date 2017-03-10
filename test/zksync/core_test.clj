(ns zksync.core-test
  (:require [clojure.test :refer :all]
            [zksync.core :refer :all]
            [zksync.curator :refer [curator-framework no-retry]]
            [zookeeper :as zk]
            [zookeeper.data :as zd]
            [zookeeper.internal :as zi]
            [clojure.set :as set])
  (:import [org.apache.curator.test TestingServer]
           (java.util.concurrent LinkedBlockingQueue)
           (org.apache.curator.framework.recipes.cache TreeCache TreeCacheListener)
           (org.apache.curator.framework CuratorFrameworkFactory)
           (org.apache.curator.retry ExponentialBackoffRetry RetryNTimes)
           (org.apache.zookeeper KeeperException$NodeExistsException)))

(def ^:private ^TestingServer zk-server nil)

(defn connect-string [] (str "127.0.0.1:" (.getPort zk-server)))

(defn setup-embedded-zk [f]
  (let [server (TestingServer.)]
    (alter-var-root #'zk-server (constantly server))
    (f)
    (.close server)
    (alter-var-root #'zk-server (constantly nil))))

(defn clear-embedded-zk [f]
  (let [c (zk/connect (connect-string))]
    (doseq [child (remove #{"zookeeper"} (zk/children c "/"))]
      (zk/delete-all c (str "/" child)))
    (zk/close c)
    (f)))

(use-fixtures :once setup-embedded-zk)
(use-fixtures :each clear-embedded-zk)

(defmethod assert-expr 'eventually [_ form-with-keyword]
  "Asserts that given predicate is eventually true.
   It will try predicate multiple times with 50 ms delay between retries.
   After one second it will return failure in same way normal clojure.test assertion does."
  (let [form (second form-with-keyword)
        args (rest form)
        pred (first form)]
    `(let [[success?# last-values#]
           (loop [time-elapsed# 0]
             (let [values# (list ~@args)
                   result# (apply ~pred values#)]
               (if result#
                 [true values#]
                 (if (< time-elapsed# 1000)
                   (do (Thread/sleep 50) (recur (+ 50 time-elapsed#)))
                   [false values#]))))]
       (if success?#
         (do-report {:type     :pass
                     :message  "Predicate passed"
                     :expected '~form
                     :actual   (cons ~pred last-values#)})
         (do-report {:type     :fail
                     :message  "Predicate failed after trying multiple times within 1000 ms."
                     :expected '~form,
                     :actual   (list '~'not (cons '~pred last-values#))})))))

(defn- start-sync []
  (let [syncer (create (curator-framework (connect-string) (no-retry))
                       ["/root"]
                       [(curator-framework (connect-string) (no-retry) :namespace "writer")])]
    (start syncer)
    syncer))

(defn- run-sync
  ([f]
   (run-sync identity f))
  ([pre f]
   (let [c (zk/connect (connect-string))]
     (pre c)
     (let [syncer (start-sync)]
       (try
         (f c)
         (finally
           (stop syncer)
           (zk/close c)))))))

(deftest updating-structure-edge-case
  (run-sync
    (fn [c]
      (zk/create-all c "/root/a/b/c/d" :persistent? true)
      (is (eventually (zk/exists c "/root/a/b/c/d")))
      (is (eventually (zk/exists c "/writer/root/a/b/c/d")))

      (zk/delete-all c "/root/a")
      (is (eventually (nil? (zk/children c "/writer/root")))))))

(deftest creating-initial-structure
  (run-sync
    (fn [c]
      (zk/create-all c "/root/hello/world" :persistent? true))
    (fn [c]
      (is (eventually (zk/exists c "/writer/root/hello/world"))))))

(deftest updating-structure
  (run-sync
    (fn [c]
      (zk/create c "/root" :persistent? true)
      (is (eventually (zk/exists c "/writer/root")))

      (zk/delete c "/root")
      (is (eventually (not (zk/exists c "/writer/root"))))

      (zk/create c "/root" :persistent? true)
      (is (eventually (zk/exists c "/writer/root")))

      (zk/create-all c "/root/a/b/c/d" :persistent? true)
      (is (eventually (zk/exists c "/writer/root/a/b/c/d")))

      (zk/delete-all c "/root/a")
      (is (eventually (nil? (zk/children c "/writer/root")))))))

(deftest updating-data
  (run-sync
    (fn [c]
      (zk/create c "/root" :data (zd/to-bytes "hello") :persistent? true))
    (fn [c]
      (is (eventually (zk/exists c "/writer/root")))
      (is (= "hello" (zd/to-string (:data (zk/data c "/writer/root")))))

      (zk/create-all c "/root/a/b/c/d" :persistent? true)
      (is (eventually (zk/exists c "/writer/root/a/b/c/d")))
      (zk/set-data c "/root/a/b/c/d" (zd/to-bytes "sync faster!") -1)
      (is (eventually (= "sync faster!" (zd/to-string (or (:data (zk/data c "/writer/root/a/b/c/d")) (.getBytes "")))))))))
