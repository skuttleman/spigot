(ns spigot.impl.context
  (:refer-clojure :exclude [resolve])
  (:require
    [clojure.walk :as walk]
    [spigot.impl.utils :as spu]))

(def ^:dynamic *sub-ctx* {})

(defmulti resolve-param
          "Extension point for defining a param resolver."
          (fn [expr _ctx]
            (if (seqable? expr)
              (first expr)
              :default)))

(defn resolve-params [params ctx]
  (when params
    (walk/postwalk (fn [x]
                     (cond-> x
                       (list? x) (-> (resolve-param (merge ctx *sub-ctx*)))))
                   params)))

(defn merge-ctx
  ([ctx mapping result]
   (merge-ctx ctx mapping result #(do %2)))
  ([ctx mapping result update-fn]
   (reduce (fn [ctx [k result->]]
             (let [v (resolve-params result-> result)]
               (if-let [ns (namespace k)]
                 (update-in ctx [ns (symbol (name k))] update-fn v)
                 (update ctx k update-fn v))))
           ctx
           mapping)))

(defmethod resolve-param :default
  [value _]
  value)

(defmethod resolve-param 'spigot/get
  [[_ key] ctx]
  (get ctx key))

(defmethod resolve-param 'spigot/nth
  [[_ value idx] _]
  (nth value idx))
