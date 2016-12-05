(ns dev
  "Tools for interactive development with the REPL. This file should
  not be included in a production build of the application."
  (:require
    [clojure.tools.namespace.repl :refer [refresh refresh-all]]
    [zksync.core :refer :all]))

(set! *warn-on-reflection* true)
