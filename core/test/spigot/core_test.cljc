(ns ^:unit spigot.core-test
  (:require
    [clojure.set :as set]
    [clojure.test :refer [are deftest is testing]]
    [spigot.core :as sp])
  #?(:clj
     (:import
       (java.util Date))))

(defn ^:private submap? [m1 m2]
  (set/subset? (set m1) (set m2)))

(defn ^:private ->ks-pair [n]
  (let [name (str "task-" n)]
    [(keyword name) (symbol (str "?" name))]))

(defn ^:private params [max]
  (into '{:seed (spigot/get ?seed)}
        (map (fn [n]
               (let [[k sym] (->ks-pair n)]
                 [k (list 'spigot/get sym)])))
        (range max)))

(defn ^:private task [n]
  (let [[k sym] (->ks-pair n)]
    [k {:spigot/in  (params n)
        :spigot/out {sym '(spigot/get :result)}}]))

(defn ^:private ->executor [calls]
  (fn [[tag params]]
    (swap! calls assoc tag (dissoc params :spigot/id))
    {:result tag}))

(def ^:private plan
  [:spigot/serial
   [:spigot/serial
    (task 0)
    (task 1)
    [:spigot/parallel
     (task 2)
     [:spigot/serial
      (task 3)
      (task 4)]]]
   [:spigot/parallel
    [:spigot/parallel
     (task 5)
     (task 6)]
    [:spigot/serial
     (task 7)
     (task 8)]]
   (task 9)])

(deftest run-all-test
  (testing "runs a workflow to completion"
    (let [calls (atom {})
          wf (-> plan
                 (sp/create '{?seed :seed-value})
                 (sp/run-all (->executor calls)))]
      (testing "runs all tasks"
        (is (= 10 (count @calls))))

      (testing "has a complete ctx"
        (is (= '{?seed   :seed-value
                 ?task-0 :task-0
                 ?task-1 :task-1
                 ?task-2 :task-2
                 ?task-3 :task-3
                 ?task-4 :task-4
                 ?task-5 :task-5
                 ?task-6 :task-6
                 ?task-7 :task-7
                 ?task-8 :task-8
                 ?task-9 :task-9}
               (sp/context wf))))
      (testing "runs task 0"
        (is (submap? {:seed :seed-value}
                     (:task-0 @calls))))

      (testing "runs task 1"
        (is (submap? {:seed   :seed-value
                      :task-0 :task-0}
                     (:task-1 @calls))))

      (testing "runs task 2"
        (is (submap? {:seed   :seed-value
                      :task-0 :task-0
                      :task-1 :task-1}
                     (:task-2 @calls))))

      (testing "runs task 3"
        (is (submap? {:seed   :seed-value
                      :task-0 :task-0
                      :task-1 :task-1}
                     (:task-3 @calls))))

      (testing "runs task 4"
        (is (submap? {:seed   :seed-value
                      :task-0 :task-0
                      :task-1 :task-1
                      :task-3 :task-3}
                     (:task-4 @calls))))

      (testing "runs task 5"
        (is (submap? {:seed   :seed-value
                      :task-0 :task-0
                      :task-1 :task-1
                      :task-2 :task-2
                      :task-3 :task-3
                      :task-4 :task-4}
                     (:task-5 @calls))))

      (testing "runs task 6"
        (is (submap? {:seed   :seed-value
                      :task-0 :task-0
                      :task-1 :task-1
                      :task-2 :task-2
                      :task-3 :task-3
                      :task-4 :task-4}
                     (:task-6 @calls))))

      (testing "runs task 7"
        (is (submap? {:seed   :seed-value
                      :task-0 :task-0
                      :task-1 :task-1
                      :task-2 :task-2
                      :task-3 :task-3
                      :task-4 :task-4}
                     (:task-7 @calls))))

      (testing "runs task 8"
        (is (submap? {:seed   :seed-value
                      :task-0 :task-0
                      :task-1 :task-1
                      :task-2 :task-2
                      :task-3 :task-3
                      :task-4 :task-4
                      :task-7 :task-7}
                     (:task-8 @calls))))

      (testing "runs task 9"
        (is (submap? {:seed   :seed-value
                      :task-0 :task-0
                      :task-1 :task-1
                      :task-2 :task-2
                      :task-3 :task-3
                      :task-4 :task-4
                      :task-5 :task-5
                      :task-6 :task-6
                      :task-7 :task-7
                      :task-8 :task-8}
                     (:task-9 @calls)))))))

(defn ^:private now-ms []
  #?(:cljs    (.getTime (js/Date.))
     :default (.getTime (Date.))))

(defn ^:private match? [[tag opts] pattern]
  (if (keyword? pattern)
    (= pattern tag)
    (and (= tag (first pattern))
         (set/subset? (set (second pattern))
                      (set opts)))))

(defn ^:private filter-on [patterns]
  (fn [[tag]]
    (some (partial match? tag) patterns)))

(defn ^:private order [rel-order & patterns]
  (->> rel-order
       (filter (filter-on patterns))
       (sort-by val)
       (map key)))

(defn ^:private every-idx [rel-order & patterns]
  (into #{}
        (comp (filter (filter-on patterns))
              (map val))
        rel-order))

(defn ^:private min-idx [rel-order & patterns]
  (apply min (apply every-idx rel-order patterns)))

(defn ^:private max-idx [rel-order & patterns]
  (apply max (apply every-idx rel-order patterns)))

(defn ^:private run-plan!
  ([plan]
   (run-plan! plan {}))
  ([plan ctx]
   (let [tasks (atom [])
         wf (sp/create plan ctx)
         next-wf (sp/run-all wf (fn [task]
                                  (let [before (now-ms)
                                        after (do (Thread/sleep (+ 50 (rand-int 300)))
                                                  (now-ms))]
                                    (swap! tasks conj (update task 1 dissoc :spigot/id))
                                    {:before before :after after})))]
     {:tasks     @tasks
      :init-wf   wf
      :wf        next-wf
      :rel-order (into {}
                       (map-indexed (fn [idx task]
                                      [task idx]))
                       @tasks)})))

(deftest serial-test
  (let [plan '[:spigot/serial
               [:task {:spigot/in {:a 1}}]
               [:spigot/serialize {:spigot/for [?val [2 3 4]]}
                [:spigot/serial
                 [:task {:spigot/in {:a1 (spigot/get ?val)}}]
                 [:task {:spigot/in {:a2 (spigot/get ?val)}}]]]
               [:task {:spigot/in {:a 5}}]]]
    (testing "when running the serial workflow"
      (let [{:keys [rel-order]} (run-plan! plan)]
        (testing "runs items in order"
          (is (= [[:task {:a 1}]
                  [:task {:a1 2}]
                  [:task {:a2 2}]
                  [:task {:a1 3}]
                  [:task {:a2 3}]
                  [:task {:a1 4}]
                  [:task {:a2 4}]
                  [:task {:a 5}]]
                 (order rel-order :task))))))))

(deftest parallel-test
  (testing "when running the parallel workflow"
    (let [plan '[:spigot/parallel
                 [:task {:spigot/in  {:a 1}
                         :spigot/out {?a1 (spigot/get :after)
                                      ?b1 (spigot/get :before)}}]
                 [:spigot/parallelize {:spigot/for [?val [2 3 4]]}
                  [:spigot/parallel
                   [:task {:spigot/in  {:a1 (spigot/get ?val)}
                           :spigot/out {[?a2 (spigot/get ?val)] (spigot/get :after)
                                        [?b2 (spigot/get ?val)] (spigot/get :before)}}]
                   [:task {:spigot/in  {:a2 (spigot/get ?val)}
                           :spigot/out {[?a3 (spigot/get ?val)] (spigot/get :after)
                                        [?b3 (spigot/get ?val)] (spigot/get :before)}}]]]
                 [:task {:spigot/in  {:a 5}
                         :spigot/out {?a4 (spigot/get :after)
                                      ?b4 (spigot/get :before)}}]]
          {{:keys [ctx]} :wf :keys [tasks]} (run-plan! plan)]
      (testing "runs all tasks in parallel"
        (is (= #{[:task {:a 1}]
                 [:task {:a1 2}]
                 [:task {:a2 2}]
                 [:task {:a1 3}]
                 [:task {:a2 3}]
                 [:task {:a1 4}]
                 [:task {:a2 4}]
                 [:task {:a 5}]}
               (set tasks)))
        (is (> (apply min (map ctx '[?a1 [?a2 2] [?a2 3] [?a2 4] [?a3 2] [?a3 3] [?a3 4] ?a4]))
               (apply max (map ctx '[?b1 [?b2 2] [?b2 3] [?b2 4] [?b3 2] [?b3 3] [?b3 4] ?b4])))))))

  (testing "when running serial groups within a parallel workflow"
    (let [plan '[:spigot/parallel
                 [:task {:spigot/in  {:a 1}
                         :spigot/out {?a1 (spigot/get :after)
                                      ?b1 (spigot/get :before)}}]
                 [:spigot/parallelize {:spigot/for [?val [2 3 4]]}
                  [:spigot/serial
                   [:task {:spigot/in  {:a (spigot/get ?val) :serial 1}
                           :spigot/out {[?a2 (spigot/get ?val)] (spigot/get :after)
                                        [?b2 (spigot/get ?val)] (spigot/get :before)}}]
                   [:task {:spigot/in  {:a (spigot/get ?val) :serial 2}
                           :spigot/out {[?a3 (spigot/get ?val)] (spigot/get :after)
                                        [?b3 (spigot/get ?val)] (spigot/get :before)}}]]]
                 [:task {:spigot/in  {:a 5}
                         :spigot/out {?a4 (spigot/get :after)
                                      ?b4 (spigot/get :before)}}]]
          ctx (-> (run-plan! plan)
                  :wf
                  sp/context)]
      (testing "serialized steps are ordered"
        (is (>= (get ctx '[?b3 2])
                (get ctx '[?a2 2])))
        (is (>= (get ctx '[?b3 3])
                (get ctx '[?a2 3])))
        (is (>= (get ctx '[?b3 4])
                (get ctx '[?a2 4])))))))

(deftest combo-test
  (let [plan '[:spigot/parallel
               [:spigot/serial
                [:spigot/serial
                 [:spigot/parallelize {:spigot/for [?foo (spigot/get ?bar)]}
                  [:spigot/serial
                   [:one {:spigot/in {:foo (spigot/get ?foo)}}]
                   [:one {:spigot/in {:foo (spigot/get ?foo)}}]]]
                 [:two]]
                [:spigot/serialize {:spigot/for [?_ []]}
                 [:never]]]
               [:three]
               [:spigot/parallelize {:spigot/for [?outer (spigot/get ?nums)]}
                [:spigot/serial
                 [:spigot/parallel
                  [:four {:spigot/in {:item  (spigot/get ?outer)
                                      :items (spigot/get ?nums)}}]
                  [:spigot/serialize {:spigot/for [?inner [:a :b :c]]}
                   [:five {:spigot/in {:inner (spigot/get ?inner)
                                       :outer (spigot/get ?outer)}}]]]
                 [:six {:spigot/in {:value (spigot/get ?outer)
                                    :bar   (spigot/get ?bar)}}]]]]]
    (testing "when running a combo workflow"
      (let [{:keys [rel-order tasks]} (run-plan! plan '{?bar  ["cat" "mouse"]
                                                        ?nums [1 2 3]})]
        (testing "and when checking relative order"
          (testing "runs every task"
            (is (= #{[:one {:foo "cat"}]
                     [:one {:foo "mouse"}]
                     [:two {}]
                     [:three {}]
                     [:four {:item  1
                             :items [1 2 3]}]
                     [:four {:item  2
                             :items [1 2 3]}]
                     [:four {:item  3
                             :items [1 2 3]}]
                     [:five {:inner :a
                             :outer 1}]
                     [:five {:inner :b
                             :outer 1}]
                     [:five {:inner :c
                             :outer 1}]
                     [:five {:inner :a
                             :outer 2}]
                     [:five {:inner :b
                             :outer 2}]
                     [:five {:inner :c
                             :outer 2}]
                     [:five {:inner :a
                             :outer 3}]
                     [:five {:inner :b
                             :outer 3}]
                     [:five {:inner :c
                             :outer 3}]
                     [:six {:value 1
                            :bar   ["cat" "mouse"]}]
                     [:six {:value 2
                            :bar   ["cat" "mouse"]}]
                     [:six {:value 3
                            :bar   ["cat" "mouse"]}]}
                   (set tasks))))
          (testing "tasks are ordered correctly"
            (is (> (min-idx rel-order :six)
                   (max-idx rel-order :four)))
            (is (> (min-idx rel-order :six)
                   (max-idx rel-order :five)))
            (is (> (min-idx rel-order :two)
                   (max-idx rel-order :one)))
            (is (= [[:five {:inner :a :outer 1}]
                    [:five {:inner :b :outer 1}]
                    [:five {:inner :c :outer 1}]]
                   (order rel-order [:five {:outer 1}])))
            (is (= [[:five {:inner :a :outer 2}]
                    [:five {:inner :b :outer 2}]
                    [:five {:inner :c :outer 2}]]
                   (order rel-order [:five {:outer 2}])))
            (is (= [[:five {:inner :a :outer 3}]
                    [:five {:inner :b :outer 3}]
                    [:five {:inner :c :outer 3}]]
                   (order rel-order [:five {:outer 3}])))))))))

(deftest error-handling-test
  (testing "when an uncaught exception occurs"
    (let [ex (is (thrown? Throwable
                          (sp/run-all (sp/create '[:spigot/serial
                                                   [:fail!]
                                                   [:never]])
                                      #(throw (ex-info (str "bad" (first %)) {:no :good})))))
          wf (:wf (ex-data ex))]
      (testing "produces a workflow in an error state"
        (is (false? (sp/finished? wf)))
        (is (= {:no      :good
                :message "bad:fail!"}
               (sp/error wf)))))))
