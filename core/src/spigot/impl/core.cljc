(ns spigot.impl.core
  (:refer-clojure :exclude [next])
  (:require
    [clojure.walk :as walk]
    [spigot.impl.context :as spc]
    [spigot.impl.multis :as spm]
    [spigot.impl.utils :as spu]))

(defn ^:private handle-result! [{:keys [ctx] :as wf} task-id status value]
  (if-let [[_ {:spigot/keys [out] :as opts}] (get-in wf [:tasks task-id])]
    (if-let [existing (get-in wf [:results task-id])]
      (throw (ex-info "task already completed" {:task-id task-id
                                                :result  existing}))
      (-> wf
          (update :running disj task-id)
          (update :results assoc task-id [status value])
          (cond->
            (= :success status)
            (update :ctx spc/merge-ctx out value opts))))
    (throw (ex-info "unknown task" {:task-id task-id}))))

(defn gen-id []
  (random-uuid))

(defn normalize [form]
  (let [[tag & [opts? :as more]] form
        [opts & children] (cond->> more
                            (not (map? opts?)) (cons {}))]
    (into [tag (assoc opts :spigot/id (gen-id))]
          (map normalize)
          children)))

(defn build-tasks [[_ {task-id :spigot/id} & children :as task]]
  (into {task-id (spu/contract-task task)}
        (map build-tasks)
        children))

(defn all-ids [[_ {task-id :spigot/id} & children]]
  (into #{task-id} (map all-ids) children))

(defn create [form ctx]
  (let [[_ {root-id :spigot/id} :as form] (normalize form)]
    {:ctx     ctx
     :root-id root-id
     :tasks   (build-tasks form)
     :running #{}
     :results {}}))

(defn finished? [{:keys [root-id] :as wf}]
  (boolean (spm/task-finished? wf (spu/expand-task wf root-id))))

(defn next [{:keys [ctx error root-id] :as wf}]
  (if (or error (finished? wf))
    [wf #{}]
    (let [[next-wf tasks] (spm/next-runnable wf (spu/expand-task wf root-id))]
      [next-wf (into #{}
                     (map (fn [[_ opts :as task]]
                            (let [{task-id :spigot/id :spigot/keys [in]} opts]
                              (assoc task 1 (-> in
                                                (spc/resolve-with-sub-ctx ctx opts)
                                                (assoc :spigot/id task-id))))))
                     tasks)])))

(defn succeed [wf task-id data]
  (handle-result! wf task-id :success data))

(defn fail [wf task-id ex-data]
  (-> wf
      (handle-result! task-id :failure ex-data)
      (assoc :error ex-data)))
