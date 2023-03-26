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

(defmethod handle-task :sleeper
  [[_ {[lo hi] :range}]]
  (Thread/sleep (+ lo (rand-int (- hi lo)))))

(def ^:private results
  (atom []))

(defmethod handle-task :conj
  [[_ params]]
  (swap! results conj (some-> params (dissoc :results))))

(defn task-runner [[tag {:keys [operands]} :as task]]
  (let [operation (apply list tag operands)]
    (println "BEGINNING" task)
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

(def dynamic-plan
  '[:spigot/parallelize {:spigot/for [?i [:a :b :c]]}
    [:spigot/serialize {:spigot/for [?j [1 2 3]]}
     [:spigot/parallel
      [:conj {:spigot/in {:tuple [(spigot/item ?i) (spigot/item ?j)]}}]
      [:conj {:spigot/in {:i (spigot/item ?i)
                          :j (spigot/item ?j)}}]]]])

(comment
  (-> plan
      (sp/create '{?a 3
                   ?b 2})
      (sp/run-all task-runner)
      sp/context)

  (do (reset! results [])
      (-> dynamic-plan
          sp/create
          (sp/run-all task-runner)
          (some-> sp/error clojure.pprint/pprint))
      @results))
