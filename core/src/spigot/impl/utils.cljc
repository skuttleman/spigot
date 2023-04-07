(ns spigot.impl.utils)

(def ^:dynamic *debug?* false)

(defn ^:private gen-id []
  (if *debug?*
    (gensym "task_")
    (random-uuid)))

(defn task->id
  "Extract a task's unique id"
  [[_ {task-id :spigot/id}]]
  task-id)

(defn ^:private task->scope-key [task]
  (str "spigot.id:" (task->id task)))

(defn destroy-sub-scope [wf task]
  (let [sub-key (task->scope-key task)]
    (cond-> wf
      (not *debug?*)
      (update :sub-scope dissoc sub-key))))

(defn get-sub-scope [wf task]
  (let [scope-k (task->scope-key task)]
    (get-in wf [:sub-scope scope-k])))

(defn normalize
  "Enforce \"opts\" maps and add unique ids to a tree of hiccup."
  [form]
  (let [[tag & [opts? :as more]] form
        [opts & children] (cond->> more
                            (not (map? opts?)) (cons {}))]
    (into [tag (assoc opts :spigot/id (gen-id))]
          (map normalize)
          children)))

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

(defn ^:private update-when [m k f & f-args]
  (if (contains? m k)
    (apply update m k f f-args)
    m))

(defn namespace-params [task]
  (let [ns (task->scope-key task)]
    (letfn [(ns-fn [sym]
              (symbol ns (name sym)))]
      (walk-opts task #(-> %
                           (update-when :spigot/out update-keys ns-fn)
                           (update-when :spigot/into update-keys ns-fn)
                           (update-when :spigot/commit update-keys ns-fn))))))
