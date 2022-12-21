(ns spigot.core.utils
  (:require
    [spigot.core :as sp]))

(defn run-sync
  [wf executor]
  (let [[next-wf tasks] (sp/next-sync wf)]
    (cond
      (sp/finished? next-wf) next-wf
      (seq tasks) (recur (reduce (fn [wf task]
                                   (sp/finish wf (:spigot/id task) (executor task)))
                                 next-wf
                                 tasks)
                         executor)
      :else (throw (ex-info "workflow not completable" {:workflow next-wf})))))
