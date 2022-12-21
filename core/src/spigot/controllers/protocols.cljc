(ns spigot.controllers.protocols)

(defprotocol ITaskProcessor
  (process-task [this ctx task]))

(defprotocol IWorkflowHandler
  (on-create [this ctx workflow])
  (on-update [this ctx workflow])
  (on-complete [this ctx workflow]))

(defprotocol IErrorHandler
  (on-error [this ctx ex workflow]))
