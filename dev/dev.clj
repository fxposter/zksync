(set! *warn-on-reflection* true)

(ns dev
  "Tools for interactive development with the REPL. This file should
  not be included in a production build of the application."
  (:require
    [clojure.tools.namespace.repl :refer [refresh refresh-all]]
    [zksync.core :refer :all]
    [zksync.curator :refer [curator-framework no-retry exponential-backoff-retry]]
    [clojure.test :refer [run-tests]]))

(defn t []
  (require 'zksync.core-test)
  (run-tests 'zksync.core-test))

(defn rt []
  (refresh :after `t))
