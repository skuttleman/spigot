(ns spigot.example
  (:require
    [spigot.core :as sp]))

(defn ^:private ops [& operands]
  {:spigot/in  {:operands (->> operands
                               butlast
                               (mapv (partial list 'spigot/get)))}
   :spigot/out {(last operands) '(spigot/get :result)}})

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
  #_(println "PARAMS" params))

(defmethod handle-task :sleeper
  [[_ {[lo hi] :range}]]
  (Thread/sleep (+ lo (rand-int (- hi lo)))))

(defmethod handle-task :throw!
  [_]
  (throw (ex-info "bad" {4 :u})))

(def ^:private results
  (atom []))

(defmethod handle-task :conj
  [[_ params]]
  (swap! results conj (some-> params (dissoc :results)))
  {:out params})

(defn task-runner [[tag {:keys [operands]} :as task]]
  (let [operation (apply list tag operands)]
    #_(println "BEGINNING" task)
    (let [result (handle-task task)]
      #_(println "FINISHING" (list := operation result))
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
  '[:spigot/parallelize {:spigot/for  [?i [:a :b :c]]
                         :spigot/into {?results (spigot/get ?things)}}
    [:spigot/serialize {:spigot/for  [?j [1 2 3]]
                        :spigot/into {?things (spigot/get ?thing)}}
     [:spigot/serial
      [:printer {:spigot/out {?something 17}}]
      [:spigot/parallel
       [:printer {:spigot/in {:tuple [(spigot/get ?i) (spigot/get ?j)]
                              :stuff (spigot/get ?stuff)}}]
       [:conj {:spigot/in  {:i (spigot/get ?i)
                            :j (spigot/get ?j)}
               :spigot/out {?thing (spigot/get :out)}}]]]]])

(comment
  (-> plan
      (sp/create '{?a 3
                   ?b 2})
      (sp/run-all task-runner)
      sp/context
      (= '{?a 4, ?b 2, ?f 2, ?c 96, ?d 2, ?e 112}))

  (do (reset! results [])
      (-> dynamic-plan
          sp/create
          (sp/run-all task-runner)
          (some-> sp/error clojure.pprint/pprint))
      @results)

  (-> '[:spigot/try
        [:spigot/serial
         [:throw!]]
        [:spigot/catch {:spigot/error ?ex}
         [:spigot/parallel
          [:printer {:spigot/in {:ex-data (spigot/get ?ex)}}]]]]
      sp/create
      (sp/run-all task-runner)
      (some-> sp/error clojure.pprint/pprint))
  )

;; at some point we'll get serious about taking crossbeam to the next level
;; we need to get to the next level of code-breakdownitude (wow is this in fact trerrible?)
