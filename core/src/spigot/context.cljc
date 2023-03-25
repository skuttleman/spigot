(ns spigot.context
  (:refer-clojure :exclude [resolve])
  (:require
    [clojure.walk :as walk]))

(defmulti ^:private resolve-param
          (fn [item _]
            (when (list? item)
              (first item))))

(defmethod resolve-param :default
  [list _]
  list)

(defn resolve-params [params ctx]
  (when params
    (walk/postwalk (fn [x]
                     (cond-> x
                       (list? x) (resolve-param ctx)))
                   params)))

(defn merge-ctx [ctx ->ctx result]
  (reduce (fn [ctx [param k]]
            (if-let [[_ v] (find result k)]
              (assoc ctx param v)
              ctx))
          ctx
          ->ctx))

(defmethod resolve-param 'spigot/get
  [[_ key] ctx]
  (get ctx key))

(defmethod resolve-param 'spigot/nth
  [[_ value idx] _]
  (nth value idx))
