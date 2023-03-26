(ns spigot.impl.utils)

(defn ^:private gen-id []
  (random-uuid))

(defn task->id [[_ {task-id :spigot/id}]]
  task-id)

(defn gen-tree-ids [form]
  (let [[tag & [opts? :as more]] form
        [opts & children] (cond->> more
                            (not (map? opts?)) (cons {}))]
    (into [tag (assoc opts :spigot/id (gen-id))]
          (map gen-tree-ids)
          children)))

(defn expand-task [wf task-id]
  (when-let [[tag opts & task-ids] (get (:tasks wf) task-id)]
    (into [tag opts]
          (map (partial expand-task wf))
          task-ids)))

(defn contract-task [[tag opts & children]]
  (into [tag opts]
        (map (comp :spigot/id second))
        children))

(defn build-tasks [[_ {task-id :spigot/id} & children :as task]]
  (into {task-id (contract-task task)}
        (map build-tasks)
        children))

(defn all-ids [[_ {task-id :spigot/id} & children]]
  (into #{task-id} (map all-ids) children))

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
