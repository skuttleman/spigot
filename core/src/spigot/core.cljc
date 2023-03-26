(ns spigot.core
  (:refer-clojure :exclude [next])
  (:require
    [spigot.impl.core :as spi]
    spigot.impl.ext))

(defn create
  "Create a workflow plan."
  ([plan]
   (create plan {}))
  ([plan ctx]
   (spi/create plan ctx)))

(defn context
  "Returns the current context value"
  [workflow]
  (:ctx workflow))

(defn next
  "Returns a tuple of `[updated-workflow set-of-runnable-tasks]`."
  [workflow]
  (spi/next workflow))

(defn succeed
  "processes a successful task and returns an updated workflow."
  [workflow task-id result]
  (spi/succeed workflow task-id result))

(defn fail
  "processes a failed task and returns an updated workflow."
  [workflow task-id ex-data]
  (spi/fail workflow task-id ex-data))

(defn finished?
  "have all tasks been completed (success or fail)?"
  [workflow]
  (boolean (spi/finished? workflow)))

(defn error
  "gets the workflow's unhandled error. Returns `nil` if workflow is unstarted/healthy/completed."
  [workflow]
  (:error workflow))

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
    (if (seq tasks)
      (recur (run-tasks next-wf tasks executor) executor)
      next-wf)))
