(ns spigot.impl
  (:require
    [clojure.walk :as walk]
    [spigot.base :as spb]
    [spigot.context :as sp.ctx]
    [spigot.multis :as spm]))

(defn ^:private unstarted? [{:keys [running results]} task-id]
  (not (or (contains? running task-id)
           (contains? results task-id))))

(defn ^:private task->id [[_ {task->id :spigot/id}]]
  task->id)

(defmethod spm/task-finished?-impl :default
  [wf [_ {:spigot/keys [id realized?] :as task} & tasks]]
  (and realized?
       (contains? (:results wf) id)
       (every? (partial spm/task-finished? wf) tasks)))

(defn ^:private task-wrapper-finished? [wf [_ {:spigot/keys [realized?]} & tasks]]
  (and realized?
       (every? (partial spm/task-finished? wf) tasks)))

(.addMethod spm/task-finished?-impl :spigot/serial task-wrapper-finished?)
(.addMethod spm/task-finished?-impl :spigot/serialize task-wrapper-finished?)
(.addMethod spm/task-finished?-impl :spigot/parallel task-wrapper-finished?)
(.addMethod spm/task-finished?-impl :spigot/parallelize task-wrapper-finished?)

(defmethod spm/realize-task-impl :default
  [wf [tag opts & children :as task]]
  (let [task-id (task->id task)]
    (assoc-in wf
              [:tasks task-id]
              (into [tag opts]
                    (map task->id)
                    children))))

(defn ^:private realize-expander
  [{:keys [ctx] :as wf} [tag {:spigot/keys [id in] :as opts} & [template & more]]]
  (assert (and template (empty? more))
          (str tag " supports exactly one child"))
  (let [[binding expr] (:spigot/for in)
        items (sp.ctx/resolve-params expr ctx)
        [next-wf child-ids] (reduce (fn [[wf ids] idx]
                                      (let [task (->> template
                                                      spb/normalize
                                                      (walk/postwalk (fn [x]
                                                                       (condp = x
                                                                         (list 'spigot/item binding)
                                                                         (list 'spigot/nth expr idx)

                                                                         x))))]
                                        [(update wf :tasks merge (spb/build-tasks task))
                                         (conj ids (task->id task))]))
                                    [wf []]
                                    (range (count items)))
        realized-task (into [tag opts] child-ids)]
    (-> next-wf
        (assoc-in [:tasks id] realized-task)
        (update :tasks #(apply dissoc % (spb/all-ids template))))))

(.addMethod spm/realize-task-impl :spigot/serialize realize-expander)
(.addMethod spm/realize-task-impl :spigot/parallelize realize-expander)

(defmethod spm/next-runnable-impl :default
  [wf task]
  (let [task-id (task->id task)]
    (if (unstarted? wf task-id)
      [(update wf :running conj task-id) [task]]
      [wf nil])))

(defn ^:private next-serial-tasks [{:keys [running] :as wf} [_ _ & tasks]]
  (let [[_ _ :as task] (->> tasks
                               (remove (partial spm/task-finished? wf))
                               first)
        task-id (task->id task)]
    (if (and task-id (not (contains? running task-id)))
      (spm/next-runnable wf task)
      [wf nil])))

(defn ^:private next-parallel-tasks [wf [_ _ & tasks]]
  (transduce (filter (comp (partial unstarted? wf) task->id))
             (completing
               (fn [[wf tasks] [_ _ :as task]]
                 (let [[next-wf sub-tasks] (spm/next-runnable wf task)]
                   [next-wf (into tasks sub-tasks)])))
             [wf nil]
             tasks))

(.addMethod spm/next-runnable-impl :spigot/serial next-serial-tasks)
(.addMethod spm/next-runnable-impl :spigot/serialize next-serial-tasks)
(.addMethod spm/next-runnable-impl :spigot/parallel next-parallel-tasks)
(.addMethod spm/next-runnable-impl :spigot/parallelize next-parallel-tasks)
