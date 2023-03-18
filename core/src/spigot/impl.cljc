(ns spigot.impl
  (:refer-clojure :exclude [next])
  (:require
    [spigot.context :as sp.ctx]))

(defn ^:private id-gen []
  (random-uuid))

(defn ^:private normalize [[tag & [opts? :as more]]]
  (let [[opts & children] (cond->> more
                                   (not (map? opts?)) (cons {}))
        opts (assoc opts :spigot/id (id-gen))]
    (into [tag opts] (map normalize) children)))

(defn ^:private unstarted? [{::keys [results running]} task-id]
  (not (or (contains? running task-id)
           (contains? results task-id))))

(defmulti task-finished? (fn [_ [tag]] tag))
(defmethod task-finished? :default
  [wf [_ {task-id :spigot/id} & tasks]]
  (and (contains? (::results wf) task-id)
       (every? (comp (partial task-finished? wf) (::tasks wf)) tasks)))
(defmethod task-finished? :spigot/serial
  [wf [_ _ & tasks]]
  (every? (comp (partial task-finished? wf) (::tasks wf)) tasks))
(defmethod task-finished? :spigot/parallel
  [wf [_ _ & tasks]]
  (every? (comp (partial task-finished? wf) (::tasks wf)) tasks))

(defmulti runnable (fn [_ [tag]] tag))
(defmethod runnable :default
  [wf [_ {task-id :spigot/id}]]
  (when (unstarted? wf task-id)
    #{task-id}))

(defmethod runnable :spigot/serial
  [{::keys [running tasks] :as wf} [_ _ & task-ids]]
  (let [[task-id task] (->> task-ids
                            (sequence (comp (map (juxt identity tasks))
                                            (remove (comp (partial task-finished? wf) second))))
                            first)]
    (when (and task-id (not (running task-id)))
      (runnable wf task))))

(defmethod runnable :spigot/parallel
  [{::keys [tasks] :as wf} [_ _ & task-ids]]
  (into #{}
        (comp (filter (partial unstarted? wf))
              (mapcat (comp (partial runnable wf)
                            tasks)))
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

(defn next [{::keys [root tasks] :as wf}]
  (let [task-ids (runnable wf (get tasks root))]
    [(update wf ::running into task-ids)
     (task-set wf task-ids)]))

(defn rerun [wf]
  (task-set wf (::running wf)))

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
  (task-finished? wf (get-in wf [::tasks (::root wf)])))

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
    {:spigot/version 1
     ::form form
     ::ctx ctx
     ::root root-id
     ::tasks (build-tasks form)
     ::running #{}
     ::results {}}))
