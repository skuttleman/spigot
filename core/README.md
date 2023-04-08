# Spigot Core

This is the core of the spigot workflow library.

## Use the Runner

The easier way to get start is to use the built-in runner in `spigot.runner`.

```clojure
(require '[spigot.core :as sp])
(require '[spigot.runner :as spr])

(defmulti task-runner first)

(defmethod task-runner :my-task
  [_]
  (println "I'm running!"))

(spr/run-all (sp/create [:my-task]) task-runner)
```

## Interact With the Framework Manually

Here are the steps for running the workflow broken down.

```clojure
(require '[spigot.core :as sp])
(require '[spigot.impl.utils :as spu])

(def my-plan
  '[:spigot/try
    [:spigot/serial
     [:task {:spigot/in {:step 1}}]
     [:task {:spigot/in {:step 2}}]
     [:task {:spigot/in {:step 3}}]]
    [:spigot/catch {:spigot/error ?ex-data}
     [:my-error-handler {:spigot/in {:error (spigot/get ?ex-data)}}]]])

;; Step 1 - Build a workflow from a plan
(def my-workflow (sp/create my-plan))

;; Step 2 - Update the workflow to get a set of tasks to run
(let [[next-workflow task-set] (sp/next my-workflow)]
  (def my-workflow next-workflow)

  ;; Step 2a - Run tasks in whatever order, however (local or distributed) you like and
  ;;           with (real or imaginary) task implementations
  (into {}
        (map (juxt spu/task->id magical-unicorn-fairy/run-and-give-back-some-time))
        task-set))

;; Step 3 - Update the workflow with one or more of the tasks results
(alter-var-root #'my-workflow sp/succeed! task-1-id result-data)
(alter-var-root #'my-workflow sp/fail! task-2-id (ex-data ex))

;; Step 4 - Repeat back to `Step 2` (interleaving with Step 3 as you wish/can) until all
;;          task results have been delivered and `(is (#{:success :failure} (sp/status my-workflow)))`
```

## Standard Features

### :spigot/serial and :spigot/parallel

A simple grouping of tasks that can be run in serial or parallel, respectively.

```clojure
[:spigot/serial
 [:task-1]
 [:task-2]]

[:spigot/parallel
 [:task-1]
 [:task-2]]
```

### :spigot/serialize and :spigot/parallelize

A dynamic expansion of tasks built by iterating over a value at runtime. Includes semantics for reducing values
back into the shared scope. These two tags (necessarily) surround their children with an isolated scope so no
writes leak from the subtree until the entire collection is finished. The `:spigot/into` parameter allows you
to gather items from the various sub-scopes of each child and store them in the shared scope for later use.

```clojure
'[:spigot/serialize {:spigot/for  [?item (spigot/get ?items)]
                     :spigot/into {?results (spigot/each (spigot/get ?result))}}
  [:task {:spigot/in  {:item (spigot/get ?item)}
          :spigot/out {?result (spigot/get :result)}}]]
```

### :spigot/try and :spigot/catch

Basic error handling semantics for dealing with a failed task or subtree.

```clojure
'[:spigot/try
  [:spigot/parallelize {...}
   [:some-task]]
  [:spigot/catch {:spigot/error ?ex-data}
   [:error-handler {:spigot/in {:error (spigot/get ?ex-data)}}]]]
```

### :spigot/isolate

Wraps a subtree and only commits expressly mapped data to the shared scope upon success.

```clojure
'[:spigot/serial
  [:spigot/isolate {:spigot/with   {?input 3}
                    :spigot/commit {?out-1 (spigot/get ?output)}}
   [:task {:spigot/in {:in (spigot/get ?input)}
           :spigot/out {?output (spigot/get :out)}}]]
  [:spigot/isolate {:spigot/with   {?input 7}
                    :spigot/commit {?out-2 (spigot/get ?output)}}
   [:task {:spigot/in {:in (spigot/get ?input)}
           :spigot/out {?output (spigot/get :out)}}]]]
```

## Extend The Framework

See `spigot.impl.api` for some handy utilities for building your own custom workflow directives
(i.e. `:spigot/serial`). The API can be extended by implementing multimethods in `spigot.impl.multis`.
You can also implement your own value resolvers and reducers - like `(spigot/get ...)` and
`(spigot/each ...)` by extending the multimethods in `spigot.impl.context`.

## Tests

```bash
$ clj -A:dev -m kaocha.runner 
```
