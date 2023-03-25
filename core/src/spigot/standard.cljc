(ns spigot.standard
  (:refer-clojure :exclude [next])
  (:require
   [spigot.context :as sp.ctx]))

(defn ^:private id-gen []
  (random-uuid))

(defn ^:private unstarted? [{::keys [results running]} task-id]
  (not (or (contains? running task-id)
           (contains? results task-id))))

(defn ^:private build-tasks [[tag opts & children]]
  (reduce merge
          {(:spigot/id opts) (into [tag opts]
                                   (map (comp :spigot/id second))
                                   children)}
          (map build-tasks children)))

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

(defn ^:private expand-task [wf [tag opts & task-ids]]
  (into [tag opts]
        (map (comp (partial expand-task wf) (::tasks wf)))
        task-ids))

(defn ^:private prepare-task [wf [tag {:spigot/keys [id in]} & children]]
  (into [tag (-> in
                 (sp.ctx/resolve-params (::ctx wf))
                 (assoc :spigot/id id))]
        children))

(defmethod task-finished? :spigot/serial
  [wf task]
  (wrapper-task-finished? wf task))

(defmethod task-finished? :spigot/parallel
  [wf task]
  (wrapper-task-finished? wf task))

(defmethod task-finished? :spigot/serialize
  [wf [_ {:spigot/keys [realized?]} :as task]]
  (and realized?
       (wrapper-task-finished? wf task)))

(defmethod task-finished? :spigot/parallelize
  [wf [_ {:spigot/keys [realized?]} :as task]]
  (and realized?
       (wrapper-task-finished? wf task)))

(defn ^:private serial-runnable-tasks [{::keys [running tasks] :as wf} [_ _ & task-ids]]
  (let [[task-id task] (->> task-ids
                            (sequence (comp (map (juxt identity tasks))
                                            (remove (comp (partial task-finished? wf) second))))
                            first)]
    (if (and task-id (not (running task-id)))
      (runnable-tasks wf task)
      [wf nil])))

(defn ^:private parallel-runnable-tasks [{::keys [tasks] :as wf} [_ _ & task-ids]]
  (transduce (filter (partial unstarted? wf))
             (completing
              (fn [[wf task-ids] task-id]
                (let [task (tasks task-id)]
                  (-> (runnable-tasks wf task)
                      (update-in [0 ::running] into task-ids)
                      (update 1 into task-ids)))))
             [wf nil]
             task-ids))

(defmethod runnable-tasks :spigot/serial
  [wf task]
  (serial-runnable-tasks wf task))

(defmethod runnable-tasks :spigot/parallel
  [wf task]
  (parallel-runnable-tasks wf task))

(defn ^:private realize-runnable-tasks [{::keys [ctx tasks] :as wf}
                                        [tag {:spigot/keys [id in] :as opts} & children]
                                        expand-fn]
  (assert (= 1 (count children))
          (str tag " supports exactly one child"))
  (let [{:keys [each items]} (sp.ctx/resolve-params in ctx)
        template
        (expand-task wf (get tasks (first children)))

        [_ opts & children :as realized-task]
        (into [tag (assoc opts :spigot/realized? true)]
              (map-indexed (fn [idx item]
                             (normalize (update-in template
                                                   [1 :spigot/aliases]
                                                   assoc
                                                   each (list 'spigot/nth (:items in) idx)
                                                   item))))
              items)

        new-tasks
        (dissoc (build-tasks realized-task) id)

        wf-task (into [tag opts]
                      (map (comp :spigot/id second))
                      children)
        next-wf
        (-> wf
            (update ::tasks merge new-tasks)
            (assoc-in [::tasks id] wf-task))]
    (expand-fn next-wf wf-task)))

(defmethod runnable-tasks :spigot/serialize
  [wf [_ {:spigot/keys [realized?]} :as task]]
  (if realized?
    (serial-runnable-tasks wf task)
    (realize-runnable-tasks wf task serial-runnable-tasks)))

(defmethod runnable-tasks :spigot/parallelize
  [wf [_ {:spigot/keys [realized?]} :as task]]
  (if realized?
    (parallel-runnable-tasks wf task)
    (realize-runnable-tasks wf task parallel-runnable-tasks)))

(defn ^:private task-set [wf task-ids xform]
  (into #{}
        (comp xform
              (map (partial prepare-task wf))
              (map (partial expand-task wf))
              (map (::tasks wf)))
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
