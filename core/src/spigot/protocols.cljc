(ns spigot.protocols
  (:refer-clojure :exclude [next resolve]))

(defprotocol ICtxResolver
  (resolve [this ctx]))

(defprotocol IWorkflow
  (context [wf] "Returns the current context value")
  (next [workflow] "Returns a tuple of `[updated-workflow set-of-runnable-tasks]`.")
  (succeed [workflow task-id result] "processes a successful task and returns an updated workflow.")
  (fail [workflow task-id ex-data] "processes a failed task and returns an updated workflow.")
  (finished? [workflow] "have all tasks been completed?"))
