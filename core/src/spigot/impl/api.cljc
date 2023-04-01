(ns spigot.impl.api
  (:require
    [spigot.impl.utils :as spu]))

(defn contracted-task
  ([wf]
   (contracted-task wf (:root-id wf)))
  ([wf task-id]
   (get-in wf [:tasks task-id])))

(defn expanded-task
  ([wf]
   (expanded-task wf (:root-id wf)))
  ([wf task-id]
   (when-let [[tag opts & task-ids] (contracted-task wf task-id)]
     (into [tag opts]
           (map (partial expanded-task wf))
           task-ids))))

(defn expanded->contracted [[tag opts & children]]
  (into [tag opts] (map #(cond-> % (vector? %) spu/task->id)) children))

(defn expanded->tasks [[_ _ & children :as task]]
  (into {(spu/task->id task) (expanded->contracted task)}
        (map #(when (vector? %)
                (expanded->tasks %)))
        children))

(defn merge-tasks [wf task-tree]
  (update wf :tasks merge (expanded->tasks task-tree)))

(defn running? [wf task-id]
  (contains? (:running wf) task-id))

(defn context [wf]
  (:ctx wf))

(defn sub-context [wf task-id]
  (let [sub-ctx-k (spu/task->sub-ctx-k (contracted-task wf task-id))]
    (get-in wf [:sub-ctx sub-ctx-k])))

(defn destroy-sub-context [wf task-id]
  (let [sub-ctx-k (spu/task->sub-ctx-k (contracted-task wf task-id))]
    (update wf :sub-ctx dissoc sub-ctx-k)))

(defn error [wf]
  (:error wf))

(defn create [plan ctx]
  (let [root-task (spu/normalize plan)]
    {:root-id (spu/task->id root-task)
     :ctx     ctx
     :sub-ctx {}
     :tasks   (expanded->tasks root-task)
     :running #{}
     :results {}}))
