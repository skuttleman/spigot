(ns spigot.impl.context
  (:refer-clojure :exclude [resolve])
  (:require
    [clojure.walk :as walk]
    [spigot.impl.utils :as spu]))

(defn ^:private dispatch-fn [expr _value]
  (if (seqable? expr)
    (first expr)
    :default))

(defmulti value-resolver
          "Extension point for defining a value resolver.

           (value-resolver '(spigot/get ...) {...})."
          #'dispatch-fn)

(defmulti value-reducer
          "Extension point for defining a value reducer.

           (value-reducer '(spigot/each ...) [{...}])."
          #'dispatch-fn)

(def ^:dynamic *ctx* {})

(defmacro with-ctx [ctx & body]
  `(if-some [ctx# ~ctx]
     (binding [*ctx* (merge *ctx* ctx#)]
       ~@body)
     (do ~@body)))

(defn resolve-into [params ctx]
  (when params
    (walk/postwalk (fn [x]
                     (cond-> x
                       (list? x) (-> (value-resolver (merge ctx *ctx*)))))
                   params)))

(defn ^:private ->ctx [wf mapping resolve-fn]
  (reduce (fn [wf [k expr]]
            (let [v (resolve-fn expr)]
              (if-let [ns (namespace k)]
                (assoc-in wf [:sub-ctx ns (symbol (name k))] v)
                (assoc-in wf [:ctx k] v))))
          wf
          mapping))

(defn merge-ctx [wf mapping result]
  (->ctx wf mapping #(resolve-into % result)))

(defn reduce-ctx [wf mapping results]
  (->ctx wf mapping #(value-reducer % results)))

(defmethod value-resolver 'spigot/get
  [[_ key] ctx]
  (get ctx key))

(defmethod value-resolver 'spigot/nth
  [[_ value idx] _]
  (nth value idx))

(defmethod value-reducer 'spigot/each
  [[_ expr] ctxs]
  (map (partial resolve-into expr) ctxs))
