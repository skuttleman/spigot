(ns spigot.controllers.kafka.topologies
  (:require
    [spigot.controllers.protocols :as sp.pcon]
    [spigot.controllers.kafka.streams :as sp.ks]
    [spigot.core :as sp]
    [spigot.impl.api :as spapi]
    [taoensso.timbre :as log])
  (:import
    (org.apache.kafka.streams StreamsBuilder)))

(defmulti ^:private workflow-aggregator
          (fn [_ [_ [tag]]]
            tag))

(defmethod workflow-aggregator ::create!
  [_ [_ [_ params ctx]]]
  (let [init (-> (:workflows/form params)
                 (sp/create (:workflows/ctx params))
                 (merge (dissoc params :workflows/form :workflows/ctx)))
        [wf tasks] (sp/next init)]
    {:wf    wf
     :ctx   ctx
     :tasks tasks
     :init  init}))

(defmethod workflow-aggregator ::result
  [{:keys [ctx wf]} [_ [_ {:spigot/keys [id result]}]]]
  (let [[wf' tasks] (-> wf
                        (sp/succeed! id result)
                        sp/next)]
    {:wf    wf'
     :ctx   ctx
     :tasks tasks}))

(defn ^:private workflow-flat-mapper [handler [workflow-id {:keys [wf ctx tasks init]}]]
  (log/debug "processing workflow" ctx (spapi/scope wf))
  (if (= :success (sp/status wf))
    (when-let [[complete-event err-event] (sp.pcon/on-complete handler ctx wf)]
      [[workflow-id [::event (or complete-event err-event)]]])
    (let [[create-event err-event] (some->> init (sp.pcon/on-create handler ctx))
          [update-event err-event] (if init
                                     [nil err-event]
                                     (sp.pcon/on-update handler ctx wf))]
      (if err-event
        [[workflow-id [::event err-event]]]
        (cond->> (map (fn [[_ {:spigot/keys [id]} :as task]]
                        [id [::task {:ctx         ctx
                                     :task        task
                                     :workflow    wf
                                     :workflow-id workflow-id}]])
                      tasks)
          update-event (cons [workflow-id [::event update-event]])
          create-event (cons [workflow-id [::event create-event]]))))))

(defn ^:private task-flat-mapper [handler [spigot-id {:keys [ctx task workflow workflow-id]}]]
  (log/debug "processing task" ctx spigot-id task)
  (let [[result ex] (sp.pcon/process-task handler ctx task)
        result {:spigot/id     spigot-id
                :spigot/result result}]
    (if ex
      (when-let [event (sp.pcon/on-error handler ctx ex workflow)]
        [[workflow-id [::event event]]])
      [[workflow-id [::workflow [::result result]]]])))

(defn ^:private ->safe-handler [handler]
  (reify
    sp.pcon/IWorkflowHandler
    (on-create [this ctx workflow]
      (try [(sp.pcon/on-create handler ctx workflow)]
           (catch Throwable ex
             [nil (sp.pcon/on-error this ctx ex workflow)])))
    (on-update [this ctx workflow]
      (try [(sp.pcon/on-update handler ctx workflow)]
           (catch Throwable ex
             [nil (sp.pcon/on-error this ctx ex workflow)])))
    (on-complete [this ctx workflow]
      (try [(sp.pcon/on-complete handler ctx workflow)]
           (catch Throwable ex
             [nil (sp.pcon/on-error this ctx ex workflow)])))

    sp.pcon/ITaskProcessor
    (process-task [_ ctx task]
      (try [(sp.pcon/process-task handler ctx task)]
           (catch Throwable ex
             [nil ex])))

    sp.pcon/IErrorHandler
    (on-error [_ ctx ex workflow]
      (try (sp.pcon/on-error handler ctx ex workflow)
           (catch Throwable _
             nil)))))

(defn workflow-manager-topology
  [^StreamsBuilder builder {:keys [handler event-topic-cfg task-topic-cfg workflow-topic-cfg]}]
  (let [handler (->safe-handler handler)
        stream (-> builder
                   (sp.ks/stream workflow-topic-cfg)
                   (sp.ks/group-by-key workflow-topic-cfg)
                   (sp.ks/aggregate (constantly nil) workflow-aggregator workflow-topic-cfg)
                   .toStream
                   (sp.ks/flat-map (partial workflow-flat-mapper handler)))]
    (-> stream
        (sp.ks/filter (comp #{::task} first second))
        (sp.ks/map-values second)
        (sp.ks/to task-topic-cfg))
    (when event-topic-cfg
      (-> stream
          (sp.ks/filter (comp #{::event} first second))
          (sp.ks/map-values second)
          (sp.ks/to event-topic-cfg)))))

(defn task-processor-topology
  [^StreamsBuilder builder {:keys [handler event-topic-cfg task-topic-cfg workflow-topic-cfg]}]
  (let [handler (->safe-handler handler)
        stream (-> builder
                   (sp.ks/stream task-topic-cfg)
                   (sp.ks/flat-map (partial task-flat-mapper handler)))]
    (-> stream
        (sp.ks/filter (comp #{::workflow} first second))
        (sp.ks/map-values second)
        (sp.ks/to workflow-topic-cfg))
    (when event-topic-cfg
      (-> stream
          (sp.ks/filter (comp #{::event} first second))
          (sp.ks/map-values second)
          (sp.ks/to event-topic-cfg)))))
