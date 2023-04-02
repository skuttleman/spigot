(ns spigot.runner
  (:require
    [spigot.core :as sp]
    [spigot.impl.utils :as spu]))

(defn ^:private run-task [executor task]
  (let [task-id (spu/task->id task)]
    (try [task-id (executor task)]
         (catch Throwable ex
           [task-id
            nil
            (-> ex
                ex-data
                (update :message #(or %
                                      (not-empty (ex-message ex))
                                      (str (class ex)))))]))))

(defn ^:private handle-task-result [wf [task-id result ex-data]]
  (if ex-data
    (sp/fail! wf task-id ex-data)
    (sp/succeed! wf task-id result)))

(defn run-tasks
  "Runs `tasks` through the `executor` in parallel and collects the results into an
   updated workflow. Will throw if one or more tasks throws."
  [wf tasks executor]
  (cond
    (= 1 (count tasks)) (handle-task-result wf (run-task executor (first tasks)))
    (seq tasks) (let [started-tasks (mapv #(future (run-task executor %)) tasks)]
                  (transduce (map deref)
                             (completing handle-task-result)
                             wf
                             started-tasks))
    :else wf))

(defn run-all
  "Run through all tasks in a single process. Useful for testing and prototyping.
   `executor` is a function that takes the context-resolved task expression
   (i.e. [:task-id {:resolved :input-params}])."
  [wf executor]
  (let [[next-wf tasks] (sp/next wf)
        wf-status (sp/status next-wf)]
    (cond
      (= :failure wf-status) (throw (ex-info "workflow failed" {:wf next-wf}))
      (or (= :success wf-status) (empty? tasks)) next-wf
      :else (recur (run-tasks next-wf tasks executor) executor))))
