(ns spigot.impl.multis
  (:require
    [spigot.impl.api :as spapi]
    [spigot.impl.utils :as spu]
    [spigot.impl.context :as spc]))

(defn ^:private dispatch-fn [_ [tag]]
  tag)

(defmulti realize-task-impl
          "Extension point for realizing a task.
           Do not invoke directly. Use [[realize-task]] instead."
          #'dispatch-fn)

(defn realize-task [wf [_ {:spigot/keys [realized?]} :as task]]
  (if realized?
    wf
    (let [realized-task (assoc-in task [1 :spigot/realized?] true)
          next-wf (spapi/merge-tasks wf realized-task)]
      (realize-task-impl next-wf realized-task))))

(defmulti next-runnable-impl
          "Extension point for generating a set of the next tasks to be run.
           Do not invoke directly. Use [[next-runnable]] instead."
          #'dispatch-fn)

(defn next-runnable [wf [_ {:spigot/keys [realized? finalized?]} :as task]]
  (cond
    finalized? [wf nil]
    realized? (next-runnable-impl wf task)
    :else (let [next-wf (realize-task wf task)
                task (spapi/expanded-task next-wf (spu/task->id task))]
            (next-runnable-impl next-wf task))))

(defmulti task-status-impl
          "Extension point for determining if a task status. Implemenation should return one of
           #{:init :running :success :failure}
           Do not invoke directly. Use [[task-status]] instead."
          #'dispatch-fn)

(defn task-status [wf [_ _ :as task]]
  (task-status-impl wf task))

(defmulti finalize-tasks-impl
          "Extension point for finalizing a task.
           Do not invoke directly. Use [[finalize-task]] instead."
          #'dispatch-fn)

(defn finalize-tasks [wf [_ {:spigot/keys [finalized?]} :as task]]
  (if (or finalized? (not (contains? #{:failure :success} (task-status wf task))))
    wf
    (let [finalized-task (assoc-in task [1 :spigot/finalized?] true)]
      (-> wf
          (spapi/merge-tasks finalized-task)
          (finalize-tasks-impl finalized-task)))))

(defmulti contextualize-impl
          "Extension point for building sub context around task parameterization.
           Do not invoke directly. Use [[contextualize]] instead."
          (fn [_wf _target-ids [tag]]
            tag))

(defn contextualize [wf target-ids [_ opts :as task]]
  (cond-> (if (:spigot/realized? opts)
            (contextualize-impl wf target-ids task)
            #{})
    (contains? target-ids (spu/task->id task))
    (conj (assoc task 1 (-> (:spigot/in opts)
                            (spc/resolve-into (spapi/context wf))
                            (assoc :spigot/id (spu/task->id task)))))))
