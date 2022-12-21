# Spigot Kafka

A kafka controller for distributing spigot workflows across a kafka topology.

### Example

```clojure
(require '[spigot.controllers.kafka.core :as sp.kafka])
(require '[spigot.controllers.kafka.common :as sp.kcom])
(require '[spigot.controllers.protocols :as sp.pcon])

;; build topologies
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

;; run topologies
(sp.kafka/start! wf-topology
                 {:application.id "wf-application-id"
                  :brokers        "..."})

(sp.kafka/start! task-topology
                 {:application.id "task-application-id"
                  :brokers        "..."})


;; initiate a workflow
(def producer (KafkaProducer. ...))

(let [workflow-id (java.util.UUID/randomUUID)
      wf-plan [:spigot/serial
               [:task-1 {:spigot/in '{:input (sp.ctx/get ?initial)}}]
               [:task-2]]]
  (.publish producer
            (ProducerRecord. "workflow-topic"
                             workflow-id
                             (sp.kafka/create-wf-msg {:workflows/form wf-plan
                                                      :workflows/ctx  '{?initial :ctx}}
                                                     {:workflow/id workflow-id}))))
```

## Tests

```bash
$ clj -A:dev -m kaocha.runner 
```
