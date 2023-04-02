(ns spigot.impl.multis
  "Extend these multimethods to add functionality to spigot™"
  (:require
    [spigot.impl.api :as spapi]
    [spigot.impl.utils :as spu]
    [spigot.impl.context :as spc]))

(defn ^:private dispatch-fn [_ [tag]]
  tag)

(defmulti realize-task-impl
          "Extension point for realizing a task. Called once before its first contextualization.
           Any data setup or workflow re-org can be done here.
           Do not invoke directly. Use [[realize-task]] instead.

           (realize-task-impl wf task) => next-wf"
          #'dispatch-fn)

(defn realize-task [wf [_ {:spigot/keys [realized?]} :as task]]
  (if realized?
    wf
    (let [realized-task (assoc-in task [1 :spigot/realized?] true)
          next-wf (spapi/merge-tasks wf realized-task)]
      (realize-task-impl next-wf realized-task))))

(defmulti startable-tasks-impl
          "Extension point for generating a coll of the tasks to be run. Will be called on
           all :init and :running tasks. Tasks that are already running will be filtered out
           automatically.
           Do not invoke directly. Use [[startable-tasks]] instead.

           (startable-tasks-impl wf task) => [next-wf [...startable-task-ids]]"
          #'dispatch-fn)

(defn startable-tasks [wf [_ {:spigot/keys [realized? finalized?]} :as task]]
  (cond
    finalized? [wf nil]
    realized? (startable-tasks-impl wf task)
    :else (let [next-wf (realize-task wf task)
                task (spapi/expanded-task next-wf (spu/task->id task))]
            (startable-tasks-impl next-wf task))))

(defmulti task-status-impl
          "Extension point for determining if a task status.
           Do not invoke directly. Use [[task-status]] instead.

           (task-status-impl wf task) => (:init|:running|:success|:failure)"
          #'dispatch-fn)

(defn task-status [wf [_ _ :as task]]
  (task-status-impl wf task))

(defmulti finalize-tasks-impl
          "Extension point for finalizing a task. Called once after a task is completed.
           Any data cleanup or workflow re-org can be done in this phase.
           Do not invoke directly. Use [[finalize-task]] instead.

           (finalize-tasks-impl wf task) => next-wf"
          #'dispatch-fn)

(defn finalize-tasks [wf [_ {:spigot/keys [finalized?]} :as task]]
  (if (or finalized? (not (contains? #{:failure :success} (task-status wf task))))
    wf
    (let [finalized-task (assoc-in task [1 :spigot/finalized?] true)]
      (-> wf
          (spapi/merge-tasks finalized-task)
          (finalize-tasks-impl finalized-task)))))

(defmulti contextualize-impl
          "Extension point for building [[spigot.impl.context/*ctx*]] around task
           parameterization, before being collected to be run.
           Do not invoke directly. Use [[contextualize]] instead.

           (contextualize-impl wf task)
             => (binding [spigot.impl.context/*ctx* ...]
                  (reduce (comp ... contextualize) wf children))"
          #'dispatch-fn)

(defn contextualize [wf [_ opts :as task]]
  (-> (if (:spigot/realized? opts)
        (contextualize-impl wf task)
        #{})
      (conj (assoc task 1 (-> (:spigot/in opts)
                              (spc/resolve-into (spapi/scope wf))
                              (assoc :spigot/id (spu/task->id task)))))))
