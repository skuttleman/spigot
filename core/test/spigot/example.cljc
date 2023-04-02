(ns spigot.example
  (:require
    [spigot.core :as sp]
    [spigot.impl.context :as spc]
    [spigot.runner :as spr]
    [spigot.impl.api :as spapi]))

(defmulti handle-task (fn [[tag]] tag))

(defn ^:private math [f]
  (fn [[_ {:keys [operands]}]]
    (Thread/sleep (+ (rand-int 333) 333))
    {:result (apply f operands)}))

(.addMethod handle-task :+ (math +))

(.addMethod handle-task :- (math -))

(.addMethod handle-task :* (math *))

(.addMethod handle-task :/ (math /))

(defmethod handle-task :noop
  [_])

(defmethod handle-task :printer
  [[_ params]]
  (println "PARAMS" params))

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
    (println "BEGINNING" task)
    (let [result (handle-task task)]
      (println "FINISHING" (list := operation result))
      result)))

(def math-plan
  '[:spigot/serial
    [:spigot/serialize {:spigot/for  [?i (spigot/get ?i's)]
                        :spigot/into {?results (spigot/each (spigot/get ?result))}}
     [:spigot/serial
      [:spigot/parallelize {:spigot/for  [?j (spigot/get ?j's)]
                            :spigot/into {?results (spigot/each (spigot/get ?result))}}
       [:spigot/serial
        [:* {:spigot/in  {:operands [3 (spigot/get ?i) (spigot/get ?j)]}
             :spigot/out {?val (spigot/get :result)}}]
        [:- {:spigot/in  {:operands [1000 (spigot/get ?val)]}
             :spigot/out {?result (spigot/get :result)}}]]]
      [:+ {:spigot/in  {:operands (spigot/get ?results)}
           :spigot/out {?result (spigot/get :result)}}]]]
    [:+ {:spigot/in  {:operands (spigot/get ?results)}
         :spigot/out {?final (spigot/get :result)}}]])

(def error-plan
  '[:spigot/try
    [:spigot/serial
     [:noop {:spigot/out {?before :BEFORE!}}]
     [:spigot/parallelize {:spigot/for [?_ [1 2 3]]}
      [:throw!]]
     [:printer {:spigot/in  {:never :NEVER}
                :spigot/out {?never :NEVER}}]]
    [:spigot/catch {:spigot/error ?ex-data}
     [:printer {:spigot/in  {:error (spigot/get ?ex-data)}
                :spigot/out {?handled? true}}]]])

(comment
  (-> math-plan
      (sp/create '{?i's [1 2 3]
                   ?j's [4 5 6]})
      (spr/run-all task-runner)
      spapi/scope
      (get '?final)
      (= 8730))

  (-> error-plan
      sp/create
      (spr/run-all task-runner)
      spapi/scope)

  (letfn [(plan [expr]
            [:spigot/serial
             [:spigot/parallelize '{:spigot/for  [?i (spigot/get ?outer)]
                                    :spigot/into {?ys (custom/+ (spigot/get ?yys))
                                                  ?zs (spigot/each (spigot/get ?zs))}}
              [:spigot/parallel
               [:spigot/parallelize '{:spigot/for  [?j (spigot/get ?inner)]
                                      :spigot/into {?yys (custom/+ (spigot/get ?y))
                                                    ?zs  (spigot/each (spigot/get ?z))}}
                [:spigot/serial
                 [:spigot/parallel
                  [:spigot/serial
                   [:task {:spigot/in  '{:i (spigot/get ?i)
                                         :j (spigot/get ?j)}
                           :spigot/out {'?y expr
                                        '?z :gonzo}}]]]]]]]])]
    (defmethod spc/value-reducer 'custom/+
      [[_ expr] values]
      (transduce (map (partial spc/resolve-into expr)) + 0 values))

    (-> [:spigot/parallel
         [:spigot/isolate '{:spigot/bind   {?inputs (spigot/get ?inputs)}
                            :spigot/convey {?out-1 (spigot/get ?ys)}}
          (plan '(spigot/get :i))]
         [:spigot/isolate '{:spigot/bind   {?inputs (spigot/get ?inputs)}
                            :spigot/convey {?out-2 (spigot/get ?ys)}}
          (plan '(spigot/get :j))]]
        (sp/create '{?outer [1 2 8]
                     ?inner [4 5 6 7]})
        (spr/run-all second)
        spapi/scope)))