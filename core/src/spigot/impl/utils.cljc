(ns spigot.impl.utils)

(defn ^:private gen-id []
  (random-uuid))

(defmacro spy [form]
  `(let [result# ~form]
     (println "SPY" '~form "=>")
     (clojure.pprint/pprint result#)
     result#))

(defn task->id [[_ {task-id :spigot/id}]]
  task-id)

(defn sub-ctx-k [task]
  (str "spigot.id:" (task->id task)))

(defn normalize [form]
  (let [[tag & [opts? :as more]] form
        [opts & children] (cond->> more
                            (not (map? opts?)) (cons {}))]
    (into [tag (assoc opts :spigot/id (gen-id))]
          (map normalize)
          children)))

(defn expand-task
  ([wf]
   (expand-task wf (:root-id wf)))
  ([wf task-id]
   (when-let [[tag opts & task-ids] (get (:tasks wf) task-id)]
     (into [tag opts]
           (map (partial expand-task wf))
           task-ids))))

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

(defn update-when [m k f & f-args]
  (if (contains? m k)
    (apply update m k f f-args)
    m))
