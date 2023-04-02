(ns spigot.core
  (:refer-clojure :exclude [next])
  (:require
    [clojure.string :as string]
    [spigot.impl.api :as spapi]
    [spigot.impl.context :as spc]
    [spigot.impl.multis :as spm]
    [spigot.impl.utils :as spu]
    spigot.impl.base))

(defn ^:private handle-result! [wf task-id status value]
  (let [[_ {:spigot/keys [out]} :as task] (get-in wf [:tasks task-id])
        existing (get-in wf [:results task-id])
        result [status value]]
    (cond
      (nil? task)
      (throw (ex-info "unknown task" {:task-id task-id}))

      (and existing (not= existing result))
      (throw (ex-info "task already completed" {:task-id task-id
                                                :result  existing}))

      (nil? existing)
      (-> wf
          (update :running disj task-id)
          (update :results assoc task-id result)
          (cond-> (= :success status) (spc/merge-data out value))
          (spm/finalize-tasks (spapi/expanded-task wf)))

      :else
      wf)))

(defn create
  "Create a workflow from a plan and optional initial context.

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
  (spm/task-status workflow (spapi/expanded-task workflow)))

(defn next
  "Returns a tuple of `[updated-workflow set-of-unstarted-runnable-tasks]`."
  [workflow]
  (if (#{:success :failure} (status workflow))
    [workflow #{}]
    (let [[next-wf task-ids] (spm/startable-tasks workflow (spapi/expanded-task workflow))
          task-id-set (set task-ids)
          next-wf (update next-wf :running into task-ids)
          tasks (spm/contextualize next-wf
                                   task-id-set
                                   (spapi/expanded-task next-wf))]
      (assert (and (not (contains? task-id-set nil))
                   (= task-id-set (into #{} (map spu/task->id) tasks)))
              "contextualized tasks must match the runnable set!")
      [next-wf tasks])))

(defn rerun
  "Returns a set of running tasks."
  [workflow]
  (if-let [task-ids (not-empty (:running workflow))]
    (spm/contextualize workflow
                       task-ids
                       (spapi/expanded-task workflow))
    #{}))

(defn succeed!
  "Processes a successful task and returns an updated workflow.
   Throws an exception if the task is unknown or already has a result."
  [workflow task-id data]
  (handle-result! workflow task-id :success data))

(defn fail
  "Processes a failed task and returns an updated workflow."
  [workflow task-id ex-data]
  (let [ex-data (or ex-data {})
        next-wf (handle-result! workflow task-id :failure ex-data)
        [_ {:spigot/keys [on-fail]}] (spapi/expanded-task next-wf task-id)
        [_ {:spigot/keys [finalized?]}] (some->> on-fail (spapi/expanded-task next-wf))]
    (if (and on-fail (not finalized?))
      (update-in next-wf
                 [:tasks on-fail 1 :spigot/failures]
                 (fnil conj [])
                 (assoc ex-data :spigot/id task-id))
      (assoc next-wf :error ex-data))))
