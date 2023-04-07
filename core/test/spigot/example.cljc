(ns spigot.example
  (:require
    [spigot.core :as sp]
    [spigot.impl.api :as spapi]
    [spigot.impl.context :as spc]
    [spigot.runner :as spr]))

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

(defmethod handle-task :task
  [[_ params]]
  params)

(defn task-runner [[tag {:keys [operands]} :as task]]
  (let [operation (apply list tag operands)]
    (println "BEGINNING" task)
    (let [result (handle-task task)]
      (println "FINISHING" (list := operation result))
      result)))

(comment
  ;; error handling
  (-> '[:spigot/try
        [:spigot/serial
         [:noop {:spigot/out {?before :BEFORE!}}]
         [:spigot/parallelize {:spigot/for [?_ [1 2 3]]}
          [:throw!]]
         [:printer {:spigot/in  {:never :NEVER}
                    :spigot/out {?never :NEVER}}]]
        [:spigot/catch {:spigot/error ?ex-data}
         [:printer {:spigot/in  {:error (spigot/get ?ex-data)}
                    :spigot/out {?handled? true}}]]]
      sp/create
      (spr/run-all task-runner)
      spapi/scope)


  ;; parallelized math for no good reason
  (-> '[:spigot/serial
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
             :spigot/out {?final (spigot/get :result)}}]]
      (sp/create '{?i's [1 2 3]
                   ?j's [4 5 6]})
      (spr/run-all task-runner)
      spapi/scope
      (get '?final)
      (= 8730))


  ;; isolate and reuse subtrees
  (let [sub-tree '[:spigot/parallel
                   [:spigot/serial
                    [:spigot/serialize {:spigot/for  [?i (spigot/get ?inputs)]
                                        :spigot/into {?ys (custom/+ (spigot/get ?yys))
                                                      ?zs (spigot/each (spigot/get ?zs))}}
                     [:spigot/serial
                      [:spigot/parallel
                       [:spigot/parallelize {:spigot/for  [?_ [:do :this :3x]]
                                             :spigot/into {?yys (custom/+ (spigot/get ?y))
                                                           ?zs  (spigot/each (spigot/get ?z))}}
                        [:spigot/parallel
                         [:spigot/isolate {:spigot/with   {?binding (spigot/get ?i)}
                                           :spigot/commit {?y (spigot/get ?hidden)}}
                          [:spigot/serial
                           [:task {:spigot/in  {:i (spigot/get ?binding)}
                                   :spigot/out {?hidden (spigot/get :i)
                                                ?z      :gonzo}}]]]]]]]]]]]
    (defmethod spc/value-reducer 'custom/+
      [[_ expr] values]
      (transduce (map (partial spc/resolve-into expr))
                 +
                 values))

    (-> [:spigot/parallel
         [:spigot/isolate '{:spigot/with   {?inputs (spigot/get ?as)}
                            :spigot/commit {?result (spigot/get ?ys)}}
          sub-tree]
         [:spigot/isolate '{:spigot/with   {?inputs (spigot/get ?bs)}
                            :spigot/commit {?result2 (spigot/get ?ys)}}
          sub-tree]]
        (sp/create '{?as     [1 2 8]
                     ?bs     [7 6 12 9]
                     ?inputs [1]})
        (spr/run-all task-runner)
        spapi/scope
        (select-keys '#{?inputs ?result ?result2}))))
