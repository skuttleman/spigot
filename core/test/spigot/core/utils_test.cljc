(ns ^:unit spigot.core.utils-test
  (:require
    [clojure.set :as set]
    [clojure.test :refer [are deftest is testing]]
    [spigot.core.utils :as spu]
    [spigot.core :as sp]))

(defn ^:private submap? [m1 m2]
  (set/subset? (set m1) (set m2)))

(defn ^:private ->ks-pair [n]
  (let [name (str "task-" n)]
    [(keyword name) (symbol (str "?" name))]))

(defn ^:private params [max]
  (into '{:seed (spigot/context ?seed)}
        (map (fn [n]
               (let [[k sym] (->ks-pair n)]
                 [k (list 'spigot/context sym)])))
        (range max)))

(defn task [n]
  (let [[k sym] (->ks-pair n)]
    [k {:spigot/in  (params n)
        :spigot/out {sym :result}}]))

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

(deftest run-sync-test
  (testing "runs a workflow to completion"
    (let [calls (atom {})
          wf (-> plan
                 (sp/create '{?seed :seed-value})
                 (spu/run-all (->executor calls)))]
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
