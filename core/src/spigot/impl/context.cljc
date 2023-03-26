(ns spigot.impl.context
  (:refer-clojure :exclude [resolve])
  (:require
    [clojure.walk :as walk]
    [spigot.impl.multis :as spm]))

(defmethod spm/resolve-param 'spigot/get
  [{:keys [ctx]} [_ key] _]
  (get ctx key))

(defmethod spm/resolve-param 'spigot/nth
  [_ [_ value idx] _]
  (nth value idx))

(defn resolve-params [params wf opts]
  (when params
    (walk/postwalk (fn [x]
                     (cond-> x
                       (list? x) (as-> $ (spm/resolve-param wf $ opts))))
                   params)))

(defn merge-ctx [ctx ->ctx result]
  (reduce (fn [ctx [param k]]
            (if-let [[_ v] (find result k)]
              (assoc ctx param v)
              ctx))
          ctx
          ->ctx))
