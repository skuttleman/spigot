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

(defn walk
  "Walks a normalized hiccup tree, calling `opts-fn` on the opts on the way down,
   and `form-fn` on each hiccup tree on the way up."
  [tree before-fn after-fn]
  (let [[tag opts & children] (before-fn tree)]
    (after-fn (into [tag opts]
                   (map #(walk % before-fn after-fn))
                   children))))

(defn walk-opts [tree opts-fn]
  (walk tree identity #(update % 1 opts-fn)))