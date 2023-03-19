(ns spigot.standard
  (:refer-clojure :exclude [next])
  (:require
    [spigot.context :as sp.ctx]))

(defn ^:private id-gen []
  (random-uuid))

(defn ^:private unstarted? [{::keys [results running]} task-id]
  (not (or (contains? running task-id)
           (contains? results task-id))))

(defmulti ^:private task-finished? (fn [_ [tag]] tag))

(defmethod task-finished? :default
  [wf [_ {task-id :spigot/id} & tasks]]
  (and (contains? (::results wf) task-id)
       (every? (comp (partial task-finished? wf) (::tasks wf)) tasks)))

(defn ^:private wrapper-task-finished? [wf [_ _ & tasks]]
  (every? (comp (partial task-finished? wf) (::tasks wf)) tasks))

(defmulti ^:private runnable-tasks (fn [_ [tag]] tag))
(defmethod runnable-tasks :default
  [wf [_ {task-id :spigot/id}]]
  (if (unstarted? wf task-id)
    [(update wf ::running conj task-id)
     [task-id]]
    [wf]))

(defn ^:private normalize [[tag & [opts? :as more]]]
  (let [[opts & children] (cond->> more
                            (not (map? opts?)) (cons {}))
        opts (assoc opts :spigot/id (id-gen))]
    (into [tag opts] (map normalize) children)))

(defmethod task-finished? :spigot/serial
  [wf task]
  (wrapper-task-finished? wf task))

(defmethod task-finished? :spigot/parallel
  [wf task]
  (wrapper-task-finished? wf task))

(defmethod runnable-tasks :spigot/serial
  [{::keys [running tasks] :as wf} [_ _ & task-ids]]
  (let [[task-id task] (->> task-ids
                            (sequence (comp (map (juxt identity tasks))
                                            (remove (comp (partial task-finished? wf) second))))
                            first)]
    (if (and task-id (not (running task-id)))
      (runnable-tasks wf task)
      [wf nil])))

(defmethod runnable-tasks :spigot/parallel
  [{::keys [tasks] :as wf} [_ _ & task-ids]]
  (transduce (filter (partial unstarted? wf))
             (completing
              (fn [[wf task-ids] task-id]
                (let [task (tasks task-id)]
                  (-> (runnable-tasks wf task)
                      (update-in [0 ::running] into task-ids)
                      (update 1 into task-ids)))))
             [wf nil]
             task-ids))

(defn ^:private expand-task [wf [tag opts & task-ids]]
  (into [tag opts]
        (map (comp (partial expand-task wf) (::tasks wf)))
        task-ids))

(defn ^:private task-set [wf task-ids]
  (into #{}
        (map (comp (fn [[tag {:spigot/keys [id in]} & children]]
                     (into [tag (-> in
                                    (sp.ctx/resolve-params (::ctx wf))
                                    (assoc :spigot/id id))]
                           children))
                   (partial expand-task wf)
                   (::tasks wf)))
        task-ids))

(defn next [{::keys [error root tasks] :as wf}]
  (if error
    [wf #{}]
    (let [[wf task-ids] (runnable-tasks wf (get tasks root))]
      [wf (task-set wf (distinct task-ids))])))

(defn succeed [wf task-id result]
  (if-let [[_ {:spigot/keys [out]}] (get-in wf [::tasks task-id])]
    (-> wf
        (update ::running disj task-id)
        (update ::results assoc task-id [:success result])
        (update ::ctx sp.ctx/merge-ctx out result))
    (throw (ex-info "unknown task" {:task-id task-id}))))

(defn fail [wf task-id ex-data]
  (if (get-in wf [::tasks task-id])
    (-> wf
        (update ::running disj task-id)
        (update ::results assoc task-id [:failure ex-data])
        (assoc ::error ex-data))
    (throw (ex-info "unknown task" {:task-id task-id}))))

(defn finished? [wf]
  (or (::error wf)
      (task-finished? wf (get-in wf [::tasks (::root wf)]))))

(defn ^:private build-tasks [[tag opts & children]]
  (reduce merge
          {(:spigot/id opts) (into [tag opts]
                                   (map (comp :spigot/id second))
                                   children)}
          (map build-tasks children)))

(defn context [wf]
  (::ctx wf))

(defn create [form ctx]
  (let [[_ {root-id :spigot/id} :as form] (normalize form)]
    {::form form
     ::ctx ctx
     ::root root-id
     ::tasks (build-tasks form)
     ::running #{}
     ::results {}}))
