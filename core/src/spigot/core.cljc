(ns spigot.core
  (:refer-clojure :exclude [next])
  (:require
    [spigot.impl :as sp.impl]))

(defn create
  "Create a workflow plan."
  ([form]
   (create form {}))
  ([form ctx]
   (sp.impl/create form ctx)))

(defn context [wf]
  "Returns the current context value"
  (sp.impl/context wf))

(defn next
  "Returns a tuple of `[updated-workflow set-of-runnable-tasks]`."
  [workflow]
  (sp.impl/next workflow))

(defn succeed
  "processes a successful task and returns an updated workflow."
  [workflow task-id result]
  (sp.impl/succeed workflow task-id result))

(defn fail
  "processes a failed task and returns an updated workflow."
  [workflow task-id ex-data]
  (sp.impl/fail workflow task-id ex-data))

(defn finished?
  "have all tasks been completed?"
  [workflow]
  (boolean (sp.impl/finished? workflow)))
