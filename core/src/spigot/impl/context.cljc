(ns spigot.impl.context
  (:refer-clojure :exclude [resolve])
  (:require
    [clojure.walk :as walk]
    [spigot.impl.utils :as spu]))

(def ^:dynamic ^:private *sub-ctx* {})

(defn with-sub-ctx-impl [sub-ctx f]
  (binding [*sub-ctx* (merge *sub-ctx* sub-ctx)]
    (f)))

(defmacro with-sub-ctx [sub-ctx & body]
  `(with-sub-ctx-impl ~sub-ctx (fn [] ~@body)))

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
  ([wf mapping result]
   (merge-ctx wf mapping result #(do %2)))
  ([wf mapping result update-fn]
   (reduce (fn [wf [k result->]]
             (let [v (resolve-params result-> result)]
               (if-let [ns (namespace k)]
                 (update-in wf [:sub-ctx ns (symbol (name k))] update-fn v)
                 (update-in wf [:ctx k] update-fn v))))
           wf
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
