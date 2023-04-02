(ns spigot.impl.base
  (:require
    [clojure.walk :as walk]
    [spigot.impl.api :as spapi]
    [spigot.impl.context :as spc]
    [spigot.impl.multis :as spm]
    [spigot.impl.utils :as spu]))

(defn ^:private result-status [{:keys [results]} task-id]
  (first (get results task-id)))

(defn ^:private combine-statuses [status-1 status-2]
  (cond
    (or (= :failure status-1) (= :failure status-2)) (reduced :failure)
    (= :success status-1 status-2) :success
    (= :init status-1 status-2) :init
    :else :running))

(defn ^:private reduce-status [statuses]
  (when (seq statuses)
    (reduce combine-statuses statuses)))

(defmethod spm/task-status-impl :default
  [wf [_ _ & children :as task]]
  (let [task-id (spu/task->id task)]
    (or
      (result-status wf task-id)
      (when ((:running wf) task-id)
        :running)
      (reduce-status (map (partial spm/task-status wf) children))
      :init)))

(defmethod spm/task-status-impl :spigot/try
  [wf [_ _ body handler]]
  (or (#{:init :running :success} (spm/task-status wf body))
      (#{:running :success :failure} (spm/task-status wf handler))
      :running))

(defmethod spm/realize-task-impl :default
  [wf _task]
  wf)

(defn ^:private update-when [m k f & f-args]
  (if (contains? m k)
    (apply update m k f f-args)
    m))

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
            task (namespace-params task (spu/task->sub-ctx-k task))]
        (recur (dec n)
               (spapi/merge-tasks wf task)
               (conj ids (spu/task->id task)))))))

(defmethod spm/realize-task-impl :spigot/try
  [wf [_ _ body :as task]]
  (let [task-id (spu/task->id task)]
    (spapi/merge-tasks wf (spu/walk-opts body assoc :spigot/on-fail task-id))))

(defn ^:private realize-expander
  [wf [tag {[_ expr] :spigot/for :as opts} template :as task]]
  (let [ctx (spapi/context wf)
        task-id (spu/task->id task)
        [next-wf child-ids] (expand-task-ids wf
                                             template
                                             (count (spc/resolve-into expr ctx)))
        realized-task (into [tag opts] child-ids)]
    (-> next-wf
        (assoc-in [:tasks task-id] realized-task)
        (update :tasks #(->> (spapi/contracted-task template)
                             (map spu/task->id)
                             (apply dissoc % (spu/task->id template)))))))

(.addMethod spm/realize-task-impl :spigot/serialize realize-expander)
(.addMethod spm/realize-task-impl :spigot/parallelize realize-expander)

(defmethod spm/startable-tasks-impl :default
  [wf task]
  (if (= :init (spm/task-status wf task))
    [wf [(spu/task->id task)]]
    [wf nil]))

(defmethod spm/startable-tasks-impl :spigot/try
  [wf [_ {:spigot/keys [failures]} body handler]]
  (spm/startable-tasks wf (if (seq failures) handler body)))

(defmethod spm/startable-tasks-impl :spigot/catch
  [wf [_ _ handler]]
  (spm/startable-tasks wf handler))

(defn ^:private next-serial-tasks [wf [_ _ & tasks]]
  (let [task-statuses (map (juxt identity (partial spm/task-status wf)) tasks)]
    (loop [wf wf
           [[task status] :as task-statuses] task-statuses]
      (cond
        (or (empty? task-statuses) (= :failure status))
        [wf nil]

        (= :success status)
        (recur (spm/finalize-tasks wf task) (rest task-statuses))

        :else (spm/startable-tasks wf task)))))

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

(.addMethod spm/startable-tasks-impl :spigot/serial next-serial-tasks)
(.addMethod spm/startable-tasks-impl :spigot/serialize next-serial-tasks)
(.addMethod spm/startable-tasks-impl :spigot/parallel next-parallel-tasks)
(.addMethod spm/startable-tasks-impl :spigot/parallelize next-parallel-tasks)

(defmethod spm/finalize-tasks-impl :default
  [wf [_ _ & tasks]]
  (reduce spm/finalize-tasks wf tasks))

(defmethod spm/finalize-tasks-impl :spigot/try
  [wf [_ {:spigot/keys [failures]} body handler :as task]]
  (let [task-id (spu/task->id task)
        next-wf (spc/with-ctx (when (seq failures) {::failures failures})
                  (cond-> (spm/finalize-tasks wf body)
                    (seq failures) (spm/finalize-tasks handler)))]
    (-> next-wf
        (update-in [:tasks task-id 1] dissoc :spigot/failures))))

(defn ^:private destroy-sub-context [wf task]
  (let [sub-ctx-k (spu/task->sub-ctx-k task)]
    (update wf :sub-ctx dissoc sub-ctx-k)))

(defn ^:private finalize-expander
  [wf [_ {:spigot/keys [into]} & tasks :as task]]
  (let [[next-wf ctxs] (reduce (fn [[wf ctxs] child]
                                 (let [next-wf (spm/finalize-tasks wf child)]
                                   (spc/with-ctx (spapi/sub-context next-wf (spu/task->id child))
                                     [(destroy-sub-context next-wf child)
                                      (conj ctxs spc/*ctx*)])))
                               [wf []]
                               tasks)]
    (spc/with-ctx (spapi/sub-context next-wf (spu/task->id task))
      (spc/reduce-ctx next-wf into ctxs))))

(.addMethod spm/finalize-tasks-impl :spigot/serialize finalize-expander)
(.addMethod spm/finalize-tasks-impl :spigot/parallelize finalize-expander)

(defmethod spm/contextualize-impl :default
  [wf target-ids [_ _ & children]]
  (into #{}
        (mapcat (partial spm/contextualize wf target-ids))
        children))

(defmethod spm/contextualize-impl :spigot/try
  [wf target-ids [_ {:spigot/keys [failures]} body handler]]
  (if (seq failures)
    (spc/with-ctx {::failures failures}
      (spm/contextualize wf target-ids handler))
    (spm/contextualize wf target-ids body)))

(defmethod spm/contextualize-impl :spigot/catch
  [wf target-ids [_ {:spigot/keys [error]} handler]]
  (spc/with-ctx (when error {error (first (::failures spc/*ctx*))})
    (spm/contextualize wf target-ids handler)))

(defn ^:private contextualize-expander
  [wf target-ids [_ opts & children]]
  (let [ctx (spapi/context wf)
        [ctx-var expr] (:spigot/for opts)]
    (into #{}
          (comp (map-indexed vector)
                (mapcat (fn [[idx child]]
                          (let [sub-ctx {ctx-var (list 'spigot/nth expr idx)}]
                            (spc/with-ctx (merge (spapi/sub-context wf (spu/task->id child))
                                                 (spc/resolve-into sub-ctx ctx))
                              (spm/contextualize wf target-ids child))))))
          children)))

(.addMethod spm/contextualize-impl :spigot/serialize contextualize-expander)
(.addMethod spm/contextualize-impl :spigot/parallelize contextualize-expander)
