(ns spigot.example
  (:require
    [spigot.core :as sp]))

(defn ^:private ops [& operands]
  {:spigot/in  {:operands (->> operands
                               butlast
                               (mapv (partial list 'spigot/get)))}
   :spigot/out {(last operands) :result}})

(defmulti handle-task (fn [[tag]] tag))

(defmethod handle-task :+
  [[_ {:keys [operands]}]]
  (Thread/sleep 555)
  {:result (apply + operands)})

(defmethod handle-task :-
  [[_ {:keys [operands]}]]
  (Thread/sleep 666)
  {:result (apply - operands)})

(defmethod handle-task :*
  [[_ {:keys [operands]}]]
  (Thread/sleep 777)
  {:result (apply * operands)})

(defmethod handle-task :/
  [[_ {:keys [operands]}]]
  (Thread/sleep 888)
  {:result (apply / operands)})

(defmethod handle-task :printer
  [[_ params]]
  (println "PARAMS" params))

(defn task-runner [[tag {:keys [operands]} :as task]]
  (let [operation (apply list tag operands)]
    (println "BEGINNING" operation)
    (let [result (handle-task task)]
      (println "FINISHING" (list := operation result))
      result)))

(def plan
  [:spigot/serial
   [:spigot/serial
    [:+ (ops '?a '?b '?a)]
    [:* (ops '?a '?b '?a)]
    [:- (ops '?a '?b '?a)]
    [:/ (ops '?a '?b '?a)]]
   [:spigot/parallel
    [:+ (ops '?a '?b '?c)]
    [:- (ops '?a '?b '?d)]
    [:* (ops '?a '?b '?e)]
    [:/ (ops '?a '?b '?f)]]
   [:spigot/parallel
    [:spigot/serial
     [:+ (ops '?a '?c '?d '?c)]
     [:* (ops '?a '?c '?d '?c)]]
    [:spigot/serial
     [:+ (ops '?a '?e '?f '?e)]
     [:* (ops '?a '?e '?f '?e)]]]])

(defn run-plan!
  ([]
   (run-plan! plan))
  ([plan]
   (run-plan! plan '{?a 1
                     ?b 2}))
  ([plan ctx]
   (-> plan
       (sp/create ctx)
       (sp/run-all task-runner)
       sp/context)))

(comment
  (run-plan! plan))

