(ns spigot.core
  (:refer-clojure :exclude [next])
  (:require
    [clojure.string :as string]
    [spigot.impl.context :as spc]
    [spigot.impl.multis :as spm]
    [spigot.impl.utils :as spu]
    spigot.impl.base))

(defn create
  "Create a workflow from a plan and optional initial context."
  ([plan]
   (create plan {}))
  ([plan ctx]
   (let [[_ {root-id :spigot/id} :as root-task] (spu/normalize plan)]
     {:ctx     ctx
      :root-id root-id
      :tasks   (spu/build-tasks root-task)
      :running #{}
      :results {}})))

(defn context
  "Returns the current context value."
  [workflow]
  (->> workflow
       :ctx
       (into {} (remove (comp string? key)))))

(defn status
  "Returns the status of the workflow. Can be one of #{:init :running :succeeded :failed}"
  [workflow]
  (spm/task-status workflow (spu/expand-task workflow)))

(defn error
  "gets the workflow's unhandled error. Returns `nil` if workflow is not in a :failed state."
  [workflow]
  (:error workflow))

(defn next
  "Returns a tuple of `[updated-workflow set-of-runnable-tasks]`."
  [{:keys [root-id] :as workflow}]
  (if (#{:success :failure} (status workflow))
    [workflow #{}]
    (let [[next-wf task-ids] (spm/next-runnable workflow (spu/expand-task workflow root-id))
          tasks (reduce (fn [tasks task-id]
                          (conj tasks (spm/contextualize next-wf task-id (spu/expand-task next-wf root-id))))
                        #{}
                        task-ids)]
      (assert (not (contains? tasks nil))
              "You done messed up")
      [next-wf tasks])))

(defn ^:private handle-result! [wf task-id status value]
  (if-let [[_ {:spigot/keys [out]}] (get-in wf [:tasks task-id])]
    (if-let [existing (get-in wf [:results task-id])]
      (throw (ex-info "task already completed" {:task-id task-id
                                                :result  existing}))
      (-> wf
          (update :running disj task-id)
          (update :results assoc task-id [status value])
          (cond->
            (= :success status)
            (-> (update :ctx spc/merge-ctx out value)
                (spm/finalize-tasks (spu/expand-task wf))))))
    (throw (ex-info "unknown task" {:task-id task-id}))))

(defn succeed
  "Processes a successful task and returns an updated workflow."
  [workflow task-id data]
  (handle-result! workflow task-id :success data))

(defn fail
  "Processes a failed task and returns an updated workflow."
  [workflow task-id ex-data]
  (let [ex-data (or ex-data {})]
    (-> workflow
        (handle-result! task-id :failure ex-data)
        (assoc :error ex-data))))

(defn ^:private run-task [executor [_ {task-id :spigot/id} :as task]]
  (try [task-id (executor task)]
       (catch Throwable ex
         [task-id nil (update (ex-data ex)
                              :message
                              #(or % (ex-message ex) (str (class ex))))])))

(defn ^:private handle-task-result [wf [task-id result ex-data]]
  (if ex-data
    (fail wf task-id ex-data)
    (succeed wf task-id result)))

(defn run-tasks
  "Runs tasks in parallel."
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
  (let [[next-wf tasks] (next wf)]
    (cond
      (error next-wf) (throw (ex-info "workflow failed" {:wf next-wf}))
      (or (= :success (status next-wf)) (empty? tasks)) next-wf
      :else (recur (run-tasks next-wf tasks executor) executor))))
