# Spigot Core

This is the core of the spigot workflow library.

## Example

Here is a simple setup using the helper utility for easily running a workflow.

```clojure
(require '[spigot.core :as sp])
(require '[spigot.core.utils :as spu])

(def my-workflow
  [:spigot/serial
   [:task/one {:spigot/in {:param (spigot/context ?seeded-value)}}]
   [:task/two {:spigot/in {:some :input}
               :spigot/out {?final :output}}]])


(def plan (sp/plan my-workflow {:ctx '{?seeded-value "some value"}}))

(defmulti executor
          (fn [{:spigot/keys [tag]}] tag))

(defmethod executor :task/one
  [{:spigot/keys [params]}]
  ;; do the thing
  nil)

(defmethod executor :task/two
  [{:spigot/keys [params]}]
  ;; do the other thing
  {:output 17})

(spu/run-sync plan executor)
;; => {?seeded-value "some value" ?final 17}
```

## Tests

```bash
$ clj -A:dev -m kaocha.runner 
```
