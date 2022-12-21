# Spigot

A small library for defining simple workflows and running them in a (potentially) distributed context.

## Examples

### Basic Workflows

A workflow definition is expressed as EDN. The simplest workflow is a single task. A task is a vector with a `keyword` denoting the
specific task implementation and an optional map.

```clojure
[:my-task]

[:my-other-task {:spigot/in {:input1 "value1" :input2 [:value 2]}}]
```

The `:spigot/in` key denotes the `params` that will be passed to the task when it's executed. You can combine tasks with
use of `:spigot/serial` and `:spigot/parallel` directives to control the order in which tasks are run.

```clojure
[:spigot/serial
 [:my-task-1]
 [:my-task-2 {:spigot/in {:some :input}}]]

[:spigot/parallel
 [:my-task-1 {:spigot/in {:some :input}}]
 [:my-task-2]]
```

The `:spigot/serial` and `:spigot/parallel` directives can be arbitrarily nested.

```clojure
[:spigot/parallel
 [:my-task-1]
 [:spigot/serial
  [:my-task-2]
  [:my-task-3]
  [:spigot/parallel
   [:my-task-4]
   [:my-task-5]]]]
```

The run order for the previous example would like this:
- `my-task-1` and `my-task-2` (in parallel)
- then `my-task-3`
- then `my-task-4` and `my-task-5` (in parallel)

### Accrued State

Spigot supports accruing data into a shared runtime context. Like the workflow definition, all runtime state must be serializable.
Return values from a task can be selectively added to a shared context and available to subsequent tasks (run order dependent).
Tasks must return a `map` of data (or `nil`).

```clojure
[:spigot/serial
 [:spigot/parallel
  [:task-one {:spigot/in {:input "a string"}
              :spigot/out {?ctx-binding :key-from-return-value}}]
  [:task-two {:spigot/out {?ctx-binding-2 :another-key}}]]
 [:task-three {:spigot/in {:param1 (sp.ctx/get ?ctx-binding)
                           :param2 (sp.ctx/get ?ctx-binding-2)}}]]
```

In this example, `task-one` returns a map which contains the key `:key-from-return-value` and `task-two` returns a map
which contains the key `:another-key`. The `:spigot/out` directive instructs the workflow to make those parts of the return
values available in the context named `?ctx-binding` and `?ctx-binding-2` respectively. When `task-three` is run, it will be
invoked with those two values pulled out of the context and passed in as `:param1` and `:param2` respectively.