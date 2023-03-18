(ns spigot.core.utils
  (:require
    [spigot.core :as sp]))

(defn run-all
  [wf executor]
  (let [[next-wf tasks] (sp/next wf)]
    (cond (sp/finished? next-wf) next-wf
          (seq tasks)
          (recur (reduce (fn [wf [task-id result ex-data]]
                           (let [next-wf (if ex-data
                                           (sp/fail wf task-id ex-data)
                                           (sp/succeed wf task-id result))]
                             next-wf))
                         next-wf
                         (pmap (fn [[_ {task-id :spigot/id} :as form]]
                                 (try [task-id (executor form)]
                                      (catch Throwable ex
                                        [task-id nil
                                         (update (ex-data ex)
                                                 :message
                                                 #(or % (ex-message ex)))])))
                               tasks))
                 executor)
          :else (throw (ex-info "workflow not completable"
                                {:workflow next-wf})))))
