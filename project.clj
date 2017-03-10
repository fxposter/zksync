(defproject zksync "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [zookeeper-clj "0.9.4" :scope "test"]
                 [org.apache.curator/curator-test "2.12.0" :scope "test"]
                 [org.apache.curator/curator-recipes "2.12.0"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]]
                   :source-paths ["dev"]
                   :repl-options {:init-ns dev}}})
