(ns spigot.impl.multis
  "Extend these multimethods to add functionality to spigot™"
  (:require
    [spigot.impl.api :as spapi]
    [spigot.impl.utils :as spu]
    [spigot.impl.context :as spc]))

(defn ^:private dispatch-fn [_ [tag]]
  tag)

(defmulti task-status-impl
          "Extension point for determining if a task status.
           Do not invoke directly. Use [[task-status]] instead.

           (task-status-impl wf task) => (:init|:running|:success|:failure)"
          #'dispatch-fn)

(defn task-status
  ([wf]
   (task-status wf (spapi/expanded-task wf)))
  ([wf [_ _ :as task]]
   (task-status-impl wf task)))

(defmulti realize-tasks-impl
          "Extension point for realizing a task. Called once the first time.
           Any data setup or workflow re-org can be done here.
           Do not invoke directly. Use [[realize-task]] instead.

           (realize-tasks-impl wf task) => next-wf"
          #'dispatch-fn)

(defn realize-tasks
  "Realizes a step. Called by the framework the first time a step is asked for its
   startable tasks."
  [wf [_ {:spigot/keys [realized?]} :as task]]
  (if realized?
    wf
    (let [realized-task (assoc-in task [1 :spigot/realized?] true)
          next-wf (spapi/merge-tasks wf realized-task)]
      (realize-tasks-impl next-wf realized-task))))

(defmulti startable-tasks-impl
          "Extension point for generating a coll of the tasks to be run. Will be called on
           all :init and :running tasks. Tasks that are already running will be filtered out
           automatically.
           Do not invoke directly. Use [[startable-tasks]] instead.

           (startable-tasks-impl wf task) => [next-wf [...startable-task-ids]]"
          #'dispatch-fn)

(defn startable-tasks
  ([wf]
   (startable-tasks wf (spapi/expanded-task wf)))
  ([wf [_ {:spigot/keys [finalized? realized?]} :as task]]
   (cond
     finalized? [wf nil]
     realized? (startable-tasks-impl wf task)
     :else (let [next-wf (realize-tasks wf task)
                 task (spapi/expanded-task next-wf (spu/task->id task))]
             (startable-tasks-impl next-wf task)))))

(defmulti contextualize-impl
          "Extension point for building [[spigot.impl.context/*ctx*]] around task
           parameterization, before being collected to be run.
           Do not invoke directly. Use [[contextualize]] instead.

           (contextualize-impl wf task) => [...contextualized-tasks]"
          #'dispatch-fn)

(defn contextualize
  ([wf]
   (contextualize wf (spapi/expanded-task wf)))
  ([wf [_ opts :as task]]
   (-> (contextualize-impl wf task)
       (conj (assoc task 1 (-> (:spigot/in opts)
                               (spc/resolve-into (spapi/scope wf))
                               (assoc :spigot/id (spu/task->id task))))))))

(defmulti finalize-tasks-impl
          "Extension point for finalizing a task. Called once after a task is completed.
           Any data cleanup or workflow re-org can be done in this phase.
           Do not invoke directly. Use [[finalize-task]] instead.

           (finalize-tasks-impl wf task) => next-wf"
          #'dispatch-fn)

(defn finalize-tasks
  ([wf]
   (finalize-tasks wf (spapi/expanded-task wf)))
  ([wf [_ {:spigot/keys [finalized?]} :as task]]
   (if finalized?
     wf
     (let [finished? (contains? #{:failure :success} (task-status wf task))
           finalized-task (cond-> task
                            finished? (assoc-in [1 :spigot/finalized?] true))]
       (-> wf
           (finalize-tasks-impl finalized-task)
           (cond-> finished? (spapi/merge-tasks finalized-task)))))))

;;
;; Default Implementations
;;

(defn combine-statuses [status-1 status-2]
  (cond
    (or (= :failure status-1) (= :failure status-2)) (reduced :failure)
    (= :success status-1 status-2) :success
    (= :init status-1 status-2) :init
    :else :running))

(defn ^:private result-status [{:keys [results]} task-id]
  (first (get results task-id)))

(defn ^:private reduce-status [statuses]
  (when (seq statuses)
    (reduce combine-statuses statuses)))

(defmethod task-status-impl :default
  [wf [_ _ & children :as task]]
  (let [task-id (spu/task->id task)]
    (or
      (result-status wf task-id)
      (when ((:running wf) task-id)
        :running)
      (reduce-status (map (partial task-status wf) children))
      :init)))

(defmethod realize-tasks-impl :default
  [wf [_ _ & children]]
  (reduce realize-tasks wf children))

(defmethod startable-tasks-impl :default
  [wf task]
  (if (= :init (task-status wf task))
    [wf [(spu/task->id task)]]
    [wf nil]))

(defmethod contextualize-impl :default
  [wf [_ _ & children]]
  (into []
        (mapcat (partial contextualize wf))
        children))

(defmethod finalize-tasks-impl :default
  [wf [_ _ & tasks]]
  (reduce finalize-tasks wf tasks))
