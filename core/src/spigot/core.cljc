(ns spigot.core
  (:refer-clojure :exclude [next])
  (:require
    [clojure.string :as string]
    [spigot.impl.api :as spapi]
    [spigot.impl.context :as spc]
    [spigot.impl.multis :as spm]
    [spigot.impl.utils :as spu]
    spigot.impl.base))

(defn create
  "Create a workflow from a plan and optional initial scope.

  (create '[:spigot/serial
            [:my-task-1]
            [:my-task-2 {:spigot/in {:arg (spigot/get ?arg)}}]]
          '{?arg \"arg\"})"
  ([plan]
   (create plan {}))
  ([plan data]
   (spapi/create plan data)))

(defn status
  "Returns the status of the workflow. Can be one of #{:init :running :success :failure}"
  [workflow]
  (spm/task-status workflow))

(defn ^:private task-set [wf task-id-set]
  (into #{}
        (filter (comp task-id-set spu/task->id))
        (spm/contextualize wf)))

(defn next
  "Returns a tuple of `[updated-workflow set-of-unstarted-runnable-tasks]`."
  [workflow]
  (if (#{:success :failure} (status workflow))
    [(spm/finalize-tasks workflow) #{}]
    (let [[next-wf task-ids] (-> workflow
                                 spm/finalize-tasks
                                 spm/startable-tasks)
          task-id-set (set task-ids)
          next-wf (update next-wf :running into task-ids)
          tasks (some->> task-id-set not-empty (task-set next-wf))]
      (assert (and (not (contains? task-id-set nil))
                   (= task-id-set (into #{} (map spu/task->id) tasks)))
              "contextualized tasks must match the runnable set!")
      [next-wf tasks])))

(defn rerun
  "Returns a set of running tasks."
  [workflow]
  (task-set workflow (:running workflow)))

(defn ^:private handle-result! [wf task-id result]
  (let [[_ _ :as task] (get-in wf [:tasks task-id])
        existing (get-in wf [:results task-id])]
    (cond
      (nil? task)
      (throw (ex-info "unknown task" {:task-id task-id}))

      (and existing (not= existing result))
      (throw (ex-info "task already completed" {:task-id task-id
                                                :result  existing}))

      (nil? existing)
      (-> wf
          (update :running disj task-id)
          (update :results assoc task-id result))

      :else
      wf)))

(defn succeed!
  "Processes a successful task and returns an updated workflow.
   Throws an exception if the task is unknown or already has a result."
  [workflow task-id details]
  (let [[_ {:spigot/keys [finalized? out]}] (spapi/contracted-task workflow task-id)]
    (if finalized?
      workflow
      (-> (handle-result! workflow task-id (cond-> [:success]
                                             spu/*debug?* (conj details)))
          (spc/merge-data out details)))))

(defn fail!
  "Processes a failed task and returns an updated workflow."
  [workflow task-id details]
  (let [ex-data (or details {})
        next-wf (handle-result! workflow task-id (cond-> [:failure]
                                                   spu/*debug?* (conj details)))
        [_ {:spigot/keys [on-fail]}] (spapi/contracted-task next-wf task-id)
        handler (some->> on-fail (spapi/contracted-task next-wf))]
    (if (and handler (not (:spigot/finalized? (second handler))))
      (update-in next-wf
                 [:tasks on-fail 1 :spigot/failures]
                 (fnil conj [])
                 (assoc ex-data :spigot/id task-id))
      (update next-wf :error #(or % ex-data)))))
