(ns spigot.impl.api
  "Some standard functions for extending spigot™"
  (:require
    [spigot.impl.utils :as spu]))

(defn ^:private expanded->contracted [[tag opts & children]]
  (into [tag opts] (map #(cond-> % (vector? %) spu/task->id)) children))

(defn ^:private expanded->tasks [[_ _ & children :as task]]
  (into {(spu/task->id task) (expanded->contracted task)}
        (map #(when (vector? %)
                (expanded->tasks %)))
        children))

(defn contracted-task
  "Gets a contracted task from the workflow (children are task-ids)."
  ([wf]
   (contracted-task wf (:root-id wf)))
  ([wf task-id]
   (get-in wf [:tasks task-id])))

(defn expanded-task
  "Gets an expanded task tree from the workflow."
  ([wf]
   (expanded-task wf (:root-id wf)))
  ([wf task-id]
   (when-let [[tag opts & task-ids] (contracted-task wf task-id)]
     (into [tag opts]
           (map (partial expanded-task wf))
           task-ids))))

(defn merge-tasks
  "Merges a normalized task tree into the workflow."
  [wf task-tree]
  (update wf :tasks merge (expanded->tasks task-tree)))

(defn context
  "The current root workflow context."
  [wf]
  (:ctx wf))

(defn sub-context
  "A task's current sub context."
  [wf task-id]
  (let [sub-ctx-k (spu/task->sub-ctx-k (contracted-task wf task-id))]
    (get-in wf [:sub-ctx sub-ctx-k])))

(defn error
  "The unhandled error of the workflow (when in a :failure state)."
  [wf]
  (:error wf))

(defn create
  "Create a workflow."
  [plan ctx]
  (let [root-task (spu/normalize plan)]
    {:root-id (spu/task->id root-task)
     :ctx     ctx
     :sub-ctx {}
     :tasks   (expanded->tasks root-task)
     :running #{}
     :results {}}))
