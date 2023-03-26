(ns spigot.impl.context
  (:refer-clojure :exclude [resolve])
  (:require
    [clojure.walk :as walk]
    [spigot.impl.multis :as spm]))

(defmethod spm/resolve-param 'spigot/get
  [[_ key] ctx]
  (get ctx key))

(defmethod spm/resolve-param 'spigot/nth
  [[_ value idx] _]
  (nth value idx))

(defn resolve-params [params ctx]
  (when params
    (walk/postwalk (fn [x]
                     (cond-> x
                       (list? x) (-> (spm/resolve-param ctx))))
                   params)))

(defn merge-ctx [ctx ->ctx result]
  (reduce (fn [ctx [param k]]
            (if-let [[_ v] (find result k)]
              (assoc ctx param v)
              ctx))
          ctx
          ->ctx))
