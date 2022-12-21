# Spigot Kafka

A kafka controller for distributing spigot workflows across a kafka topology.

### Example

```clojure
(require '[spigot.controllers.kafka.core :as sp.kafka])
(require '[spigot.controllers.kafka.common :as sp.kcom])
(require '[spigot.controllers.protocols :as sp.pcon])

(def wf-topology (-> (sp.kafka/default-builder)
                     (sp.kafka/with-wf-topology
                       {:handler            (reify sp.pcon/IWorkflowHandler ... sp.pcon/IErrorHandler ...)
                        :task-topic-cfg     (sp.kcom/->topic-cfg "task-topic")
                        :workflow-topic-cfg (sp.kcom/->topic-cfg "workflow-topic")})
                     .build))

(def task-topology (-> (sp.kafka/default-builder)
                       (sp.kafka/with-task-topology
                         {:handler            (reify sp.pcon/ITaskProcessor ... sp.pcon/IErrorHandler ...)
                          :task-topic-cfg     (sp.kcom/->topic-cfg "task-topic")
                          :workflow-topic-cfg (sp.kcom/->topic-cfg "workflow-topic")})
                       .build))

(sp.kafka/start! wf-topology
                 {:application.id "wf-application-id"
                  :brokers        "..."})

(sp.kafka/start! task-topology
                 {:application.id "task-application-id"
                  :brokers        "..."})
```

## Tests

```bash
$ clj -A:dev -m kaocha.runner 
```
