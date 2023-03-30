(ns spigot.impl.base
  (:require
    [clojure.walk :as walk]
    [spigot.impl.context :as spc]
    [spigot.impl.multis :as spm]
    [spigot.impl.utils :as spu]))

(defn ^:private unstarted? [{:keys [running results]} task-id]
  (not (or (contains? running task-id)
           (contains? results task-id))))

(defn ^:private reduce-status [statuses]
  (when (seq statuses)
    (reduce (fn [result status]
              (cond
                (or (= :failure result) (= :failure status)) (reduced :failure)
                (= :success result status) :success
                (= :init result status) :init
                :else :running))
            statuses)))

(defmethod spm/task-status-impl :default
  [{:keys [running] :as wf} [_ _ & children :as task]]
  (let [task-id (spu/task->id task)]
    (or
      (first (get-in wf [:results task-id]))
      (when (running task-id)
        :running)
      (reduce-status (map (partial spm/task-status wf) children))
      :init)))

(defmethod spm/realize-task-impl :default
  [wf _task]
  wf)

(defn ^:private namespace-params [task]
  (let [ns (spu/sub-ctx-k task)]
    (letfn [(ns-fn [sym]
              (symbol ns (name sym)))]
      (spu/walk-opts task (fn [opts]
                            (-> opts
                                (spu/update-when [:spigot/for 1] ns-fn)
                                (spu/update-when :spigot/out update-keys ns-fn)
                                (spu/update-when :spigot/into update-keys ns-fn)))))))

(defn ^:private expand-task-ids [wf template n]
  (loop [n n wf wf ids []]
    (if (zero? n)
      [wf ids]
      (let [task (-> template
                     spu/normalize
                     namespace-params)]
        (recur (dec n)
               (update wf :tasks merge (spu/build-tasks task))
               (conj ids (spu/task->id task)))))))

(defn ^:private realize-expander
  [{:keys [ctx] :as wf}
   [tag {task-id :spigot/id [_ expr] :spigot/for :as opts} template]]
  (let [[next-wf child-ids] (expand-task-ids wf
                                             template
                                             (count (spc/resolve-params expr ctx)))
        realized-task (into [tag opts] child-ids)]
    (-> next-wf
        (assoc-in [:tasks task-id] realized-task)
        (update :tasks #(apply dissoc % (spu/all-ids template))))))

(.addMethod spm/realize-task-impl :spigot/serialize #'realize-expander)
(.addMethod spm/realize-task-impl :spigot/parallelize #'realize-expander)

(defmethod spm/next-runnable-impl :default
  [wf task]
  (let [task-id (spu/task->id task)]
    (if (unstarted? wf task-id)
      [(update wf :running conj task-id) [task-id]]
      [wf nil])))

(defn ^:private next-serial-tasks [{:keys [running] :as wf} [_ _ & tasks]]
  (let [[_ _ :as child] (->> tasks
                             (remove (comp #{:success :failure} (partial spm/task-status wf)))
                             first)
        task-id (spu/task->id child)]
    (if (and task-id (not (running task-id)))
      (spm/next-runnable wf child)
      [wf nil])))

(defn ^:private next-parallel-tasks
  [wf [_ _ & tasks]]
  (transduce (remove (comp #{:success :failure} (partial spm/task-status wf)))
             (completing
               (fn [[wf task-ids] [_ _ :as task]]
                 (let [[next-wf sub-task-ids] (spm/next-runnable wf task)]
                   [next-wf (into task-ids sub-task-ids)])))
             [wf nil]
             tasks))

(.addMethod spm/next-runnable-impl :spigot/serial #'next-serial-tasks)
(.addMethod spm/next-runnable-impl :spigot/serialize #'next-serial-tasks)
(.addMethod spm/next-runnable-impl :spigot/parallel #'next-parallel-tasks)
(.addMethod spm/next-runnable-impl :spigot/parallelize #'next-parallel-tasks)

(defmethod spm/finalize-tasks-impl :default
  [wf [_ _ & tasks]]
  (reduce spm/finalize-tasks wf tasks))

(defn ^:private finalize-expander
  [wf [_ {:spigot/keys [into]} & tasks :as task]]
  (reduce (fn [{:keys [sub-ctx] :as wf} child]
            (let [sub-k (spu/sub-ctx-k child)
                  {next-sub :sub-ctx :as next-wf} (spc/with-sub-ctx (get sub-ctx sub-k)
                                                    (spm/finalize-tasks wf child))]
              (spc/with-sub-ctx (merge (get next-sub sub-k)
                                       (get next-sub (spu/sub-ctx-k task)))
                (-> next-wf
                    (update :sub-ctx dissoc sub-k)
                    (spc/merge-ctx into (:ctx next-wf) (fnil conj []))))))
          wf
          tasks))

(.addMethod spm/finalize-tasks-impl :spigot/serialize #'finalize-expander)
(.addMethod spm/finalize-tasks-impl :spigot/parallelize #'finalize-expander)

(defmethod spm/contextualize-impl :default
  [wf target-ids [_ _ & children]]
  (into #{}
        (mapcat (partial spm/contextualize wf target-ids))
        children))

(defn ^:private contextualize-expander
  [{:keys [ctx] :as wf} target-id [_ opts & children]]
  (let [[ctx-var expr] (:spigot/for opts)]
    (into #{}
          (comp (map-indexed vector)
                (mapcat (fn [[idx child]]
                          (let [sub-k (spu/sub-ctx-k child)
                                sub-ctx {ctx-var (list 'spigot/nth expr idx)}]
                            (spc/with-sub-ctx (merge (get (:sub-ctx wf) sub-k)
                                                     (spc/resolve-params sub-ctx ctx))
                              (spm/contextualize wf target-id child))))))
          children)))

(.addMethod spm/contextualize-impl :spigot/serialize #'contextualize-expander)
(.addMethod spm/contextualize-impl :spigot/parallelize #'contextualize-expander)
