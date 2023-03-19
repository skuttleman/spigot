(ns spigot.core
  (:refer-clojure :exclude [next])
  (:require
    [spigot.protocols :as sps]
    [spigot.standard :as sp.std]))

(defrecord StandardWorkflow []
  sps/IWorkflow
  (context [this]
    (sp.std/context this))
  (next [this]
    (sp.std/next this))
  (succeed [this task-id result]
    (sp.std/succeed this task-id result))
  (fail [this task-id ex-data]
    (sp.std/fail this task-id ex-data))
  (finished? [this]
    (sp.std/finished? this)))

(defn create
  "Create a workflow plan."
  ([form]
   (create form {}))
  ([form ctx]
   (map->StandardWorkflow (sp.std/create form ctx))))

(defn context [wf]
  "Returns the current context value"
  (sps/context wf))

(defn next
  "Returns a tuple of `[updated-workflow set-of-runnable-tasks]`."
  [workflow]
  (sps/next workflow))

(defn succeed
  "processes a successful task and returns an updated workflow."
  [workflow task-id result]
  (sps/succeed workflow task-id result))

(defn fail
  "processes a failed task and returns an updated workflow."
  [workflow task-id ex-data]
  (sps/fail workflow task-id ex-data))

(defn finished?
  "have all tasks been completed?"
  [workflow]
  (boolean (sps/finished? workflow)))

(defn run-all
  "Run through all tasks in a single process. Useful for testing and prototyping.
   `executor` is a function that takes the context-resolved task expression."
  [wf executor]
  (let [[next-wf tasks] (next wf)]
    (cond (finished? next-wf) next-wf
          (seq tasks)
          (recur (reduce (fn [wf [task-id result ex-data]]
                           (let [next-wf (if ex-data
                                           (fail wf task-id ex-data)
                                           (succeed wf task-id result))]
                             next-wf))
                         next-wf
                         (pmap (fn [[_ {task-id :spigot/id} :as form]]
                                 (try [task-id (executor form)]
                                      (catch Throwable ex
                                        [task-id nil
                                         (update (ex-data ex)
                                                 :message
                                                 #(or % (ex-message ex)))])))
                               tasks))
                 executor)
          :else (throw (ex-info "workflow not completable"
                                {:workflow next-wf})))))
