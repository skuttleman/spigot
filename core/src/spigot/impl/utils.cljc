(ns spigot.impl.utils)

(defn ^:private gen-id []
  (random-uuid))

(defn task->id [[_ {task-id :spigot/id}]]
  task-id)

(defn task->sub-ctx-k [task]
  (str "spigot.id:" (task->id task)))

(defn normalize [form]
  (let [[tag & [opts? :as more]] form
        [opts & children] (cond->> more
                            (not (map? opts?)) (cons {}))]
    (into [tag (assoc opts :spigot/id (gen-id))]
          (map normalize)
          children)))

(defn update-when [m k f & f-args]
  (if (contains? m k)
    (apply update m k f f-args)
    m))

(defn walk
  "Walks a normalized hiccup tree, calling `inner-fn` on each hiccup form on the
   way down and `outer-fn` on each hiccup form on the way up."
  [tree inner-fn outer-fn]
  (let [[tag opts & children] (inner-fn tree)]
    (outer-fn (into [tag opts]
                    (map #(walk % inner-fn outer-fn))
                    children))))

(defn walk-opts
  "Walk over the hiccup tree and call `opts-fn` to update every opts map."
  [tree opts-fn & opts-fn-args]
  (walk tree identity #(apply update % 1 opts-fn opts-fn-args)))
