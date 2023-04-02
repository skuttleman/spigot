(ns spigot.impl.base-test
  (:require
    [clojure.test :refer [are deftest is testing]]
    [spigot.core :as sp]
    [spigot.impl.api :as spapi]
    [spigot.impl.multis :as spm]
    [spigot.impl.utils :as spu]
    spigot.impl.base))

(deftest realize-task-default-test
  (testing ":default impl"
    (let [workflow (sp/create [:task])
          result (spm/realize-task-impl workflow (spapi/expanded-task workflow))]
      (testing "has no effect"
        (is (= workflow result)))))

  (doseq [expand-tag [:spigot/serialize :spigot/parallelize]]
    (testing (str expand-tag " impl")
      (let [workflow (sp/create [expand-tag '{:spigot/for [?a (spigot/get ?items)]}
                                 '[:task {:spigot/in {:a (spigot/get ?a)}}]]
                                '{?items [1 2]})
            next-wf (spm/realize-task-impl workflow (spapi/expanded-task workflow))
            [_ _ task-1 task-2 :as result] (spapi/expanded-task next-wf)]
        (testing "rebuilds the tree"
          (is (= 3 (count (:tasks next-wf))))
          (is (= [expand-tag '{:spigot/for [?a (spigot/get ?items)]}
                  '[:task {:spigot/in {:a (spigot/get ?a)}}]
                  '[:task {:spigot/in {:a (spigot/get ?a)}}]]
                 (spu/walk-opts result dissoc :spigot/id)))
          (are [task] (= (get-in next-wf [:tasks (spu/task->id task)]) task)
            task-1
            task-2))))))

(deftest next-runnable-default-test
  (testing ":default impl"
    (testing "when the task has not been initialized"
      (let [workflow (sp/create [:task])
            [next-wf tasks] (sp/next workflow)]
        (testing "transitions the task to :running"
          (is (= :running (spm/task-status next-wf (spapi/expanded-task next-wf))))
          (is (= 1 (count tasks)))
          (is (= (spu/task->id (first tasks)) (spu/task->id (spapi/expanded-task next-wf)))))

        (testing "and when the task has been started"
          (let [[same-wf task-ids] (spm/startable-tasks next-wf (spapi/expanded-task next-wf))]
            (testing "does not transition the workflow"
              (is (= same-wf next-wf))
              (is (empty? task-ids)))

            (testing "and when the task has succeeded"
              (let [next-wf (sp/succeed! same-wf
                                         (spu/task->id (spapi/expanded-task same-wf))
                                         {:some "result"})]
                (testing "transitions the task to :success"
                  (is (= :success (spm/task-status next-wf (spapi/expanded-task next-wf))))
                  (is (empty? (second (sp/next next-wf)))))))

            (testing "and when the task has failed"
              (let [next-wf (sp/fail same-wf
                                     (spu/task->id (spapi/expanded-task same-wf))
                                     {:some "error"})]
                (testing "transitions the task to :failure"
                  (is (= :failure (spm/task-status next-wf (spapi/expanded-task next-wf))))
                  (is (empty? (second (sp/next next-wf))))))))))))

  (testing ":spigot/serial impl"
    (testing "when its first incomplete task is not running"
      (let [workflow (sp/create [:spigot/serial
                                 [:task]
                                 [:task]])
            [next-wf tasks] (sp/next workflow)
            [_ _ child-1 child-2] (spapi/expanded-task next-wf)
            task-id-1 (spu/task->id child-1)
            task-id-2 (spu/task->id child-2)]
        (testing "returns the first task"
          (is (= #{task-id-1} (into #{} (map spu/task->id tasks))))
          (is (contains? (:running next-wf) task-id-1)))

        (testing "and when its first task is running"
          (let [[same-wf task-ids] (spm/startable-tasks next-wf (spapi/expanded-task next-wf))]
            (testing "returns no tasks to run"
              (is (empty? task-ids))
              (is (= next-wf same-wf)))))

        (testing "and when the task is completed"
          (let [next-wf (transduce (map spu/task->id)
                                   (completing #(sp/succeed! %1 %2 {:some :result}))
                                   next-wf
                                   tasks)
                [next-wf tasks] (sp/next next-wf)]
            (testing "returns the next task-id to run"
              (is (= (into #{} (map spu/task->id) tasks) #{task-id-2}))
              (is (not (contains? (:running next-wf) task-id-1)))
              (is (contains? (:running next-wf) task-id-2)))

            (testing "and when all tasks are completed"
              (let [next-wf (reduce #(sp/succeed! %1 (spu/task->id %2) {:some :result}) next-wf tasks)]
                (is (empty? (second (spm/startable-tasks next-wf (spapi/expanded-task next-wf))))))))))))

  (testing ":spigot/serialize impl"
    (testing "when its first incomplete task is not running"
      (let [workflow (sp/create '[:spigot/serialize {:spigot/for [?a [1 2]]}
                                  [:task]])
            [next-wf tasks] (sp/next workflow)
            [_ _ child-1 child-2] (spapi/expanded-task next-wf)
            task-id-1 (spu/task->id child-1)
            task-id-2 (spu/task->id child-2)]
        (testing "returns the first task"
          (is (= #{task-id-1} (into #{} (map spu/task->id tasks))))
          (is (contains? (:running next-wf) task-id-1)))

        (testing "and when its first task is running"
          (let [[same-wf task-ids] (spm/startable-tasks next-wf (spapi/expanded-task next-wf))]
            (testing "returns no tasks to run"
              (is (empty? task-ids))
              (is (= next-wf same-wf)))))

        (testing "and when the task is completed"
          (let [next-wf (transduce (map spu/task->id)
                                   (completing #(sp/succeed! %1 %2 {:some :result}))
                                   next-wf
                                   tasks)
                [next-wf tasks] (sp/next next-wf)]
            (testing "returns the next task-id to run"
              (is (= (into #{} (map spu/task->id) tasks) #{task-id-2}))
              (is (not (contains? (:running next-wf) task-id-1)))
              (is (contains? (:running next-wf) task-id-2)))

            (testing "and when all tasks are completed"
              (let [next-wf (reduce #(sp/succeed! %1 (spu/task->id %2) {:some :result}) next-wf tasks)]
                (is (empty? (second (spm/startable-tasks next-wf (spapi/expanded-task next-wf))))))))))))

  (testing ":spigot/parallel impl"
    (testing "when its tasks are not running"
      (let [workflow (sp/create [:spigot/parallel
                                 [:task]
                                 [:task]
                                 [:task]])
            [next-wf tasks] (sp/next workflow)
            [_ _ & children] (spapi/expanded-task next-wf)
            [task-1-id task-2-id task-3-id] (map spu/task->id children)]
        (testing "returns all tasks"
          (is (= #{task-1-id task-2-id task-3-id} (into #{}
                                                        (map spu/task->id)
                                                        tasks)))
          (is (= #{task-1-id task-2-id task-3-id} (:running next-wf))))

        (testing "and when its first task is running"
          (let [[same-wf task-ids] (spm/startable-tasks next-wf (spapi/expanded-task next-wf))]
            (testing "returns no tasks to run"
              (is (empty? task-ids))
              (is (= next-wf same-wf)))))

        (testing "and when the tasks are completed"
          (let [next-wf (reduce #(sp/succeed! %1 (spu/task->id %2) {:some :result}) next-wf tasks)]
            (testing "returns no new tasks"
              (is (empty? (second (spm/startable-tasks next-wf (spapi/expanded-task next-wf))))))))))
    (testing "when tasks are throttled"
      (let [max-concur (loop [[next-wf tasks] (-> '[:spigot/parallel {:spigot/throttle 3}
                                                    [:task]
                                                    [:task]
                                                    [:task]
                                                    [:task]
                                                    [:task]
                                                    [:task]
                                                    [:task]
                                                    [:task]
                                                    [:task]
                                                    [:task]]
                                                  sp/create
                                                  sp/next)
                              max-concur 0]
                         (if (seq tasks)
                           (let [task (first tasks)]
                             (recur (-> next-wf
                                        (sp/succeed! (spu/task->id task) nil)
                                        sp/next
                                        (update 1 into (rest tasks)))
                                    (max max-concur (count tasks))))
                           max-concur))]
        (testing "limits the tasks it produces at once"
          (is (= 3 max-concur))))))

  (testing ":spigot/parallelize impl"
    (testing "when its tasks are not running"
      (let [workflow (sp/create '[:spigot/parallelize {:spigot/for [?a [1 2 3]]}
                                  [:task]])
            [next-wf tasks] (sp/next workflow)
            [_ _ & children] (spapi/expanded-task next-wf)
            [task-1-id task-2-id task-3-id] (map spu/task->id children)]
        (testing "returns all tasks"
          (is (= #{task-1-id task-2-id task-3-id} (into #{} (map spu/task->id) tasks)))
          (is (= #{task-1-id task-2-id task-3-id} (:running next-wf))))

        (testing "and when the tasks are completed"
          (let [next-wf (transduce (map spu/task->id)
                                   (completing #(sp/succeed! %1 %2 {:some :result}))
                                   next-wf
                                   tasks)]
            (testing "returns no new tasks"
              (is (empty? (second (spm/startable-tasks next-wf (spapi/expanded-task next-wf))))))))))

    (testing "when tasks are throttled"
      (let [max-concur (loop [[next-wf tasks] (-> '[:spigot/parallelize {:spigot/for      [?_ [1 2 3 4 4 6 7 8 9 10]]
                                                                         :spigot/throttle 7}
                                                    [:task]]
                                                  sp/create
                                                  sp/next)
                              max-concur 0]
                         (if (seq tasks)
                           (let [task (first tasks)]
                             (recur (-> next-wf
                                        (sp/succeed! (spu/task->id task) nil)
                                        sp/next
                                        (update 1 into (rest tasks)))
                                    (max max-concur (count tasks))))
                           max-concur))]
        (testing "limits the tasks it produces at once"
          (is (= 7 max-concur)))))))

(defmethod spm/task-status-impl ::init [_ _] :init)
(defmethod spm/task-status-impl ::running [_ _] :running)
(defmethod spm/task-status-impl ::success [_ _] :success)
(defmethod spm/task-status-impl ::failure [_ _] :failure)

(deftest task-status-default-test
  (testing ":default impl"
    (are [status plan] (let [wf (sp/create plan)]
                         (= status (spm/task-status wf (spapi/expanded-task wf))))
      :init [:default
             [::init]
             [::init]
             [::init]]
      :running [:default
                [::running]
                [::init]
                [::init]]
      :running [:default
                [::success]
                [::init]
                [::init]]
      :running [:default
                [::init]
                [::success]
                [::running]]
      :failure [:default
                [::init]
                [::failure]]
      :failure [:default
                [::running]
                [::failure]]
      :failure [:default
                [::success]
                [::failure]]
      :success [:default
                [::success]
                [::success]
                [::success]])))
