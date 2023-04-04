(ns spigot.impl.base
  (:require
    [clojure.walk :as walk]
    [spigot.impl.api :as spapi]
    [spigot.impl.context :as spc]
    [spigot.impl.multis :as spm]
    [spigot.impl.utils :as spu]))

(defn ^:private task->scope-key [task]
  (str "spigot.id:" (spu/task->id task)))

(defn ^:private update-when [m k f & f-args]
  (if (contains? m k)
    (apply update m k f f-args)
    m))

(defn ^:private destroy-sub-context [wf task]
  (let [sub-key (task->scope-key task)]
    (update wf :sub-scope dissoc sub-key)))

(defn ^:private sub-scope [wf task-id]
  (let [scope-k (task->scope-key (spapi/contracted-task wf task-id))]
    (get-in wf [:sub-scope scope-k])))

(defn ^:private namespace-params [task ns]
  (letfn [(ns-fn [sym]
            (symbol ns (name sym)))]
    (spu/walk-opts task #(-> %
                             (update-when :spigot/out update-keys ns-fn)
                             (update-when :spigot/into update-keys ns-fn)))))

(defn ^:private expand-task-ids [wf template n]
  (loop [n n wf wf ids []]
    (if (zero? n)
      [wf ids]
      (let [task (spu/normalize template)
            task (namespace-params task (task->scope-key task))]
        (recur (dec n)
               (spapi/merge-tasks wf task)
               (conj ids (spu/task->id task)))))))

(defn ^:private realize-expander
  [wf [tag {[_ expr] :spigot/for :as opts} template :as task]]
  (let [scope (spapi/scope wf)
        task-id (spu/task->id task)
        [next-wf child-ids] (expand-task-ids wf
                                             template
                                             (count (spc/resolve-into expr scope)))
        realized-task (into [tag opts] child-ids)]
    (-> next-wf
        (assoc-in [:tasks task-id] realized-task)
        (update :tasks #(->> (spapi/contracted-task template)
                             (map spu/task->id)
                             (apply dissoc % (spu/task->id template)))))))

(defn ^:private next-serial-tasks [wf [_ _ & tasks]]
  (let [[task status] (->> tasks
                           (map (juxt identity (partial spm/task-status wf)))
                           (remove (comp #{:success} second))
                           first)]
    (if (or (nil? task) (= :failure status))
      [wf nil]
      (spm/startable-tasks wf task))))

(defn ^:private next-parallel-tasks
  [wf [_ {:spigot/keys [throttle]} & tasks]]
  (let [xform (cond-> (remove (comp #{:success :failure}
                                    (partial spm/task-status wf)))
                throttle (comp (take throttle)))]
    (transduce xform
               (completing
                 (fn [[wf task-ids] [_ _ :as task]]
                   (let [[next-wf sub-task-ids] (spm/startable-tasks wf task)]
                     [next-wf (into task-ids sub-task-ids)])))
               [wf nil]
               tasks)))

(defn ^:private finalize-expander
  [wf [_ {:spigot/keys [into]} & tasks :as task]]
  (let [task-id (spu/task->id task)
        [next-wf scopes] (reduce (fn [[wf scopes] child]
                                   (let [next-wf (spm/finalize-tasks wf child)
                                         sub (sub-scope next-wf (spu/task->id child))]
                                     [next-wf (conj scopes sub)]))
                                 [wf []]
                                 tasks)]
    (if (every? (comp :spigot/finalized? second)
                (nnext (spapi/expanded-task next-wf task-id)))
      (spc/with-ctx (sub-scope next-wf (spu/task->id task))
        (let [next-wf (reduce destroy-sub-context next-wf tasks)]
          (spc/reduce-data next-wf into scopes)))
      next-wf)))

(defn ^:private contextualize-expander
  [wf [_ {[binding expr] :spigot/for} & children]]
  (let [scope (spapi/scope wf)]
    (into []
          (comp (map-indexed vector)
                (mapcat (fn [[idx child]]
                          (let [child-id (spu/task->id child)
                                item (nth (spc/resolve-into expr scope) idx)]
                            (spc/with-ctx (merge (sub-scope wf child-id)
                                                 {binding item})
                              (spm/contextualize wf child))))))
          children)))



(defmethod spm/task-status-impl :spigot/try
  [wf [_ _ body handler]]
  (or (#{:init :running :success} (spm/task-status wf body))
      (#{:running :success :failure} (spm/task-status wf handler))
      :running))

(defmethod spm/realize-tasks-impl :spigot/try
  [wf [_ _ body :as task]]
  (let [task-id (spu/task->id task)]
    (spapi/merge-tasks wf (spu/walk-opts body assoc :spigot/on-fail task-id))))

(defmethod spm/startable-tasks-impl :spigot/try
  [wf [_ {:spigot/keys [failures]} body handler]]
  (spm/startable-tasks wf (if (seq failures) handler body)))

(defmethod spm/finalize-tasks-impl :spigot/try
  [wf [_ {:spigot/keys [failures]} body handler :as task]]
  (let [task-id (spu/task->id task)
        next-wf (spc/with-ctx (when (seq failures) {::failures failures})
                  (cond-> (spm/finalize-tasks wf body)
                    (seq failures) (spm/finalize-tasks handler)))]
    (cond-> next-wf
        (#{:success :failure} (spm/task-status wf handler))
        (update-in [:tasks task-id 1] dissoc :spigot/failures))))

(defmethod spm/contextualize-impl :spigot/try
  [wf [_ {:spigot/keys [failures]} body handler]]
  (if (seq failures)
    (spc/with-ctx {::failures failures}
      (spm/contextualize wf handler))
    (spm/contextualize wf body)))

(defmethod spm/startable-tasks-impl :spigot/catch
  [wf [_ _ handler]]
  (spm/startable-tasks wf handler))

(defmethod spm/contextualize-impl :spigot/catch
  [wf [_ {:spigot/keys [error]} handler]]
  (spc/with-ctx (when error {error (first (::failures spc/*ctx*))})
    (spm/contextualize wf handler)))



(defmethod spm/startable-tasks-impl :spigot/serial
  [wf task]
  (next-serial-tasks wf task))



(defmethod spm/startable-tasks-impl :spigot/parallel
  [wf task]
  (next-parallel-tasks wf task))



(defmethod spm/realize-tasks-impl :spigot/serialize
  [wf task]
  (realize-expander wf task))

(defmethod spm/startable-tasks-impl :spigot/serialize
  [wf task]
  (next-serial-tasks wf task))

(defmethod spm/finalize-tasks-impl :spigot/serialize
  [wf task]
  (finalize-expander wf task))

(defmethod spm/contextualize-impl :spigot/serialize
  [wf task]
  (contextualize-expander wf task))



(defmethod spm/realize-tasks-impl :spigot/parallelize
  [wf task]
  (realize-expander wf task))

(defmethod spm/startable-tasks-impl :spigot/parallelize
  [wf task]
  (next-parallel-tasks wf task))

(defmethod spm/finalize-tasks-impl :spigot/parallelize
  [wf task]
  (finalize-expander wf task))

(defmethod spm/contextualize-impl :spigot/parallelize
  [wf task]
  (contextualize-expander wf task))


(defmethod spm/realize-tasks-impl :spigot/isolate
  [wf [_ _ child]]
  (spapi/merge-tasks wf (namespace-params child (task->scope-key child))))

(defmethod spm/startable-tasks-impl :spigot/isolate
  [wf [_ {:spigot/keys [bind]} child]]
  (spc/with-ctx (spc/resolve-into bind (spapi/scope wf))
    (spm/startable-tasks wf child)))

(defmethod spm/finalize-tasks-impl :spigot/isolate
  [wf [_ {:spigot/keys [bind convey]} child :as task]]
  (let [next-wf (spm/finalize-tasks wf child)
        sub (merge (sub-scope next-wf (spu/task->id task))
                   (sub-scope next-wf (spu/task->id child)))]
    (spc/with-ctx (merge (spc/resolve-into bind (spapi/scope next-wf)) sub)
      (-> next-wf
          (spc/merge-data convey (sub-scope next-wf (spu/task->id child)))
          (destroy-sub-context child)))))

(defmethod spm/contextualize-impl :spigot/isolate
  [wf [_ _ child]]
  (spm/contextualize wf child))
