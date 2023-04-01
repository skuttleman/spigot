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
   (let [root-task (spu/normalize plan)]
     {:root-id (spu/task->id root-task)
      :ctx     ctx
      :sub-ctx {}
      :tasks   (spu/build-tasks root-task)
      :running #{}
      :results {}})))

(defn context
  "Returns the current context value."
  [workflow]
  (let [{:keys [ctx sub-ctx]} workflow]
    (cond-> ctx
      (seq sub-ctx)
      (assoc :spigot/sub-ctx sub-ctx))))

(defn status
  "Returns the status of the workflow. Can be one of #{:init :running :succeeded :failure}"
  [workflow]
  (spm/task-status workflow (spu/expand-task workflow)))

(defn error
  "Returns the workflow's unhandled error data. Returns `nil` if the workflow is not in a :failure state."
  [workflow]
  (:error workflow))

(defn next
  "Returns a tuple of `[updated-workflow set-of-runnable-tasks]`."
  [{:keys [root-id] :as workflow}]
  (if (#{:success :failure} (status workflow))
    [workflow #{}]
    (let [[next-wf task-ids] (spm/next-runnable workflow (spu/expand-task workflow root-id))
          next-wf (update next-wf :running into task-ids)
          tasks (spm/contextualize next-wf
                                   (set task-ids)
                                   (spu/expand-task next-wf root-id))]
      (assert (= (set task-ids) (into #{} (map spu/task->id) tasks))
              "contextualized tasks much match runnable set!")
      [next-wf tasks])))

(defn ^:private handle-result! [wf task-id status value]
  (if-let [[_ {:spigot/keys [out]}] (get-in wf [:tasks task-id])]
    (if-let [existing (get-in wf [:results task-id])]
      (throw (ex-info "task already completed" {:task-id task-id
                                                :result  existing}))
      (-> wf
          (update :running disj task-id)
          (update :results assoc task-id [status value])
          (cond-> (= :success status) (spc/merge-ctx out value))
          (spm/finalize-tasks (spu/expand-task wf))))
    (throw (ex-info "unknown task" {:task-id task-id}))))

(defn succeed
  "Processes a successful task and returns an updated workflow."
  [workflow task-id data]
  (handle-result! workflow task-id :success data))

(defn fail
  "Processes a failed task and returns an updated workflow."
  [workflow task-id ex-data]
  (let [ex-data (or ex-data {})
        next-wf (handle-result! workflow task-id :failure ex-data)
        [_ {:spigot/keys [on-fail]}] (spu/expand-task next-wf task-id)
        [_ {:spigot/keys [finalized?]}] (some->> on-fail (spu/expand-task next-wf))]
    (if (and on-fail (not finalized?))
      (update-in next-wf
                 [:tasks on-fail 1 :spigot/failures]
                 (fnil conj [])
                 (assoc ex-data :spigot/id task-id))
      (assoc next-wf :error ex-data))))

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
    (fail wf task-id ex-data)
    (succeed wf task-id result)))

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
  (let [[next-wf tasks] (next wf)]
    (cond
      (error next-wf) (throw (ex-info "workflow failed" {:wf next-wf}))
      (or (= :success (status next-wf)) (empty? tasks)) next-wf
      :else (recur (run-tasks next-wf tasks executor) executor))))
