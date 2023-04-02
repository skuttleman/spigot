# Spigot

A small library for defining simple workflows and running them in a (potentially) distributed context. Build, spec, alter, and
test your workflows without changing (or even implementing) any steps!

## Examples

### Basic Workflows

A workflow definition is expressed as EDN. The simplest workflow is a single task. A task is a vector with a `keyword` denoting the
specific task implementation and an optional map.

```clojure
[:my-task]

[:my-other-task {:spigot/in {:input1 "value1"
                             :input2 [:value 2]}}]
```

**Wire up data to be shared by tasks at runtime.**

```clojure
[:spigot/serial
 [:task-1 {:spigot/in {:arg "value"}
           :spigot/out {?task-1-result (spigot/get :result-map-key)}}]
 [:task-2 {:spigot/in {:arg (spigot/get ?task-1-result)}
           :spigot/out {?task-2-result (spigot/get :result-map-key)}}]]
```

**Compose tasks into dynamic trees of control flow to dictate exactly how to run tasks.**

```clojure
[:spigot/try
 [:spigot/parallelize {:spigot/for  [?item (spigot/get ?items)]
                       :spigot/into {?results (spigot/each {:a (spigot/get ?a)
                                                            :b (spigot/get ?b)})}}
  [:spigot/serial
   [:my-iterable-task {:spigot/in  {:item (spigot/get ?item)}
                       :spigot/out {?a (spigot/get :result)}}]
   [:another-task {:spigot/out {?b (spigot/get :b)}}]]]
 [:spigot/catch {:spigot/error ?ex-data}
  [:my-error-handler {:spigot/in {:error (spigot/get ?ex-data)}}]]]
```

**Extend the framework to support your own custom directives.**

```clojure
[:spigot/serial
 [:my-ns/fist-successful
  [:task-1 {:spigot/out {?val (spigot/get :task-1-result)}}]
  [:task-2 {:spigot/out {?val (spigot/get :task-2-result)}}]
  [:task-3 {:spigot/out {?val (spigot/get :task-3-result)}}]]
 [:final-task {:spigot/in {:val (spigot/get ?val)}}]]
```

### Accrued State

Spigot supports accruing data into a shared runtime scope. Like the workflow definition, all runtime state must be serializable.
Return values from a task can be selectively added to a shared context and available to subsequent tasks (run order dependent).
Tasks must return a `map` of data (or `nil`).

```clojure
[:spigot/serial
 [:spigot/parallel
  [:task-one {:spigot/in {:input "a string"}
              :spigot/out {?ctx-binding (spigot/get :key-from-return-map)}}]
  [:task-two {:spigot/out {?ctx-binding-2 (spigot/get :another-key)}}]]
 [:task-three {:spigot/in  {:param1 (spigot/get ?ctx-binding)
                            :param2 (spigot/get ?ctx-binding-2)}
               :spigot/out {?something (spigot/get :result)}}]
 [:spigot/parallelize {:spigot/for [?val (spigot/get ?items)]
                       :spigot/into {?fours (spigot/each (spigot/get ?four))}}
  [:spigot/serial
   [:four {:spigot/out {?four (spigot/get :result)}}]
   [:five {:spigot/in {:item (spigot/get ?val)}}]]]]
```

In this example, `task-one` returns a map which contains the key `:key-from-return-map` and `task-two` returns a map
which contains the key `:another-key`. The `:spigot/out` directive instructs the workflow to make those parts of the return
values available in the context named `?ctx-binding` and `?ctx-binding-2` respectively. When `task-three` is run, it will be
invoked with those two values pulled out of the context and passed in as `:param1` and `:param2` respectively.

### Try it at the REPL

See REPL examples [here](https://github.com/skuttleman/spigot/blob/master/core/test/spigot/example.cljc).

