(ns spigot.impl.multis
  (:require
    [spigot.impl.utils :as spu]))

(defn ^:private dispatch-fn [_ [tag]]
  tag)

(defmulti task-finished?-impl
          "Extension point for determining if a task is completed (successful or otherwise).
           Do not invoke directly. Use [[task-finished?]] instead."
          #'dispatch-fn)
(defn task-finished? [wf task]
  (task-finished?-impl wf task))

(defmulti realize-task-impl
          "Extension point for realizing a task.
           Do not invoke directly. Use [[realize-task]] instead."
          #'dispatch-fn)
(defn realize-task [wf [_ {:spigot/keys [realized?]} :as form]]
  (if realized?
    wf
    (realize-task-impl wf (update form 1 assoc :spigot/realized? true))))

(defmulti next-runnable-impl
          "Extension point for generating a set of the next tasks to be run.
           Do not invoke directly. Use [[next-runnable]] instead."
          #'dispatch-fn)
(defn next-runnable [wf [_ {task-id :spigot/id :spigot/keys [realized?]} :as task]]
  (if realized?
    (next-runnable-impl wf task)
    (let [next-wf (realize-task wf task)]
      (next-runnable-impl next-wf (spu/expand-task next-wf task-id)))))

(defmulti resolve-param
          "Resolves a workflow runtime expression from the current context"
          (fn [_wf expr _opts]
            (if (seqable? expr)
              (first expr)
              :default)))
(defmethod resolve-param :default
  [_ list _]
  list)