(ns spigot.impl.base
  (:require
    [clojure.walk :as walk]
    [spigot.impl.context :as spc]
    [spigot.impl.multis :as spm]
    [spigot.impl.utils :as spu]))

(defn ^:private running? [{:keys [running] :as wf} task-id]
  (contains? running task-id))

(defn ^:private result-status [{:keys [results] :as wf} task-id]
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
      (when (running? wf task-id)
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

(defn ^:private namespace-params [task ns]
  (letfn [(ns-fn [sym]
            (symbol ns (name sym)))]
    (spu/walk-opts task #(-> %
                             (spu/update-when :spigot/out update-keys ns-fn)
                             (spu/update-when :spigot/into update-keys ns-fn)))))

(defn ^:private expand-task-ids [wf template n]
  (loop [n n wf wf ids []]
    (if (zero? n)
      [wf ids]
      (let [task (spu/normalize template)
            task (namespace-params task (spu/sub-ctx-k task))]
        (recur (dec n)
               (update wf :tasks merge (spu/build-tasks task))
               (conj ids (spu/task->id task)))))))

(defmethod spm/realize-task-impl :spigot/try
  [wf [_ _ body :as task]]
  (let [task-id (spu/task->id task)]
    (update wf :tasks merge (-> body
                                (spu/walk-opts assoc :spigot/on-fail task-id)
                                spu/build-tasks))))

(defn ^:private realize-expander
  [{:keys [ctx] :as wf} [tag {[_ expr] :spigot/for :as opts} template :as task]]
  (let [task-id (spu/task->id task)
        [next-wf child-ids] (expand-task-ids wf
                                             template
                                             (count (spc/resolve-into expr ctx)))
        realized-task (into [tag opts] child-ids)]
    (-> next-wf
        (assoc-in [:tasks task-id] realized-task)
        (update :tasks #(apply dissoc % (spu/all-ids template))))))

(.addMethod spm/realize-task-impl :spigot/serialize #'realize-expander)
(.addMethod spm/realize-task-impl :spigot/parallelize #'realize-expander)

(defmethod spm/next-runnable-impl :default
  [wf task]
  (if (= :init (spm/task-status wf task))
    (let [task-id (spu/task->id task)]
      [wf [task-id]])
    [wf nil]))

(defmethod spm/next-runnable-impl :spigot/try
  [wf [_ {:spigot/keys [failures]} body handler]]
  (spm/next-runnable wf (if (seq failures) handler body)))

(defmethod spm/next-runnable-impl :spigot/catch
  [wf [_ _ handler]]
  (spm/next-runnable wf handler))

(defn ^:private next-serial-tasks [wf [_ _ & tasks]]
  (let [[child status] (->> tasks
                            (map (juxt identity (partial spm/task-status wf)))
                            (remove (comp #{:success} second))
                            first)
        task-id (spu/task->id child)]
    (if (and task-id (not (contains? #{:failure} status)))
      (spm/next-runnable wf child)
      [wf nil])))

(defn ^:private next-parallel-tasks
  [wf [_ {:spigot/keys [throttle]} & tasks]]
  (let [xform (cond-> (remove (comp #{:success :failure}
                                    (partial spm/task-status wf)))
                throttle (comp (take throttle)))]
    (transduce xform
               (completing
                 (fn [[wf task-ids] [_ _ :as task]]
                   (let [[next-wf sub-task-ids] (spm/next-runnable wf task)]
                     [next-wf (into task-ids sub-task-ids)])))
               [wf nil]
               tasks)))

(.addMethod spm/next-runnable-impl :spigot/serial #'next-serial-tasks)
(.addMethod spm/next-runnable-impl :spigot/serialize #'next-serial-tasks)
(.addMethod spm/next-runnable-impl :spigot/parallel #'next-parallel-tasks)
(.addMethod spm/next-runnable-impl :spigot/parallelize #'next-parallel-tasks)

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

(defn ^:private finalize-expander
  [wf [_ {:spigot/keys [into]} & tasks :as task]]
  (let [[next-wf ctxs] (reduce (fn [[wf ctxs] child]
                                 (let [sub-k (spu/sub-ctx-k child)
                                       next-wf (spm/finalize-tasks wf child)]
                                   (spc/with-ctx (get (:sub-ctx next-wf) sub-k)
                                     [(update next-wf :sub-ctx dissoc sub-k)
                                      (conj ctxs spc/*ctx*)])))
                               [wf []]
                               tasks)]
    (spc/with-ctx (get (:sub-ctx next-wf) (spu/sub-ctx-k task))
      (spc/reduce-ctx next-wf into ctxs))))

(.addMethod spm/finalize-tasks-impl :spigot/serialize #'finalize-expander)
(.addMethod spm/finalize-tasks-impl :spigot/parallelize #'finalize-expander)

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
  [{:keys [ctx] :as wf} target-ids [_ opts & children]]
  (let [[ctx-var expr] (:spigot/for opts)]
    (into #{}
          (comp (map-indexed vector)
                (mapcat (fn [[idx child]]
                          (let [sub-k (spu/sub-ctx-k child)
                                sub-ctx {ctx-var (list 'spigot/nth expr idx)}]
                            (spc/with-ctx (merge (get (:sub-ctx wf) sub-k)
                                                 (spc/resolve-into sub-ctx ctx))
                              (spm/contextualize wf target-ids child))))))
          children)))

(.addMethod spm/contextualize-impl :spigot/serialize #'contextualize-expander)
(.addMethod spm/contextualize-impl :spigot/parallelize #'contextualize-expander)
