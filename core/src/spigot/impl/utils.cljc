(ns spigot.impl.utils)

(defn task->id [[_ {task-id :spigot/id}]]
  task-id)

(defn expand-task [wf task-id]
  (when-let [[tag opts & task-ids] (get (:tasks wf) task-id)]
    (into [tag opts]
          (map (partial expand-task wf))
          task-ids)))

(defn contract-task [[tag opts & children]]
  (into [tag opts]
        (map (comp :spigot/id second))
        children))
