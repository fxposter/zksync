(defproject zksync "0.2.0"
  :description "Syncing zookeeper subtrees between multiple clusters"
  :url "https://github.com/fxposter/zksync"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [zookeeper-clj "0.9.4" :scope "test"]
                 [org.apache.curator/curator-test "2.12.0" :scope "test"]
                 [org.apache.curator/curator-recipes "2.12.0"]
                 [org.clojure/tools.logging "0.3.1"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]]
                   :source-paths ["dev"]
                   :repl-options {:init-ns dev}}})
