(ns dev
  "Tools for interactive development with the REPL. This file should
  not be included in a production build of the application."
  (:require
    [clojure.tools.namespace.repl :refer [refresh refresh-all]]
    [zksync.core :refer :all]
    [clojure.test :refer [run-tests]]))

(set! *warn-on-reflection* true)

(defn t []
  (require 'zksync.core-test)
  (run-tests 'zksync.core-test))

(defn rt []
  (refresh :after `t))