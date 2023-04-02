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

(defn resolve-into [params data]
  (when params
    (walk/postwalk (fn [x]
                     (cond-> x
                       (list? x) (-> (value-resolver (merge data *ctx*)))))
                   params)))

(defn ^:private ->data [wf mapping resolve-fn]
  (reduce (fn [wf [k expr]]
            (let [v (resolve-fn expr)]
              (if-let [ns (namespace k)]
                (assoc-in wf [:sub-scope ns (symbol (name k))] v)
                (assoc-in wf [:scope k] v))))
          wf
          mapping))

(defn merge-data [wf mapping result]
  (->data wf mapping #(resolve-into % result)))

(defn reduce-data [wf mapping results]
  (->data wf mapping #(value-reducer % results)))

(defmethod value-resolver 'spigot/get
  [[_ key] value]
  (get value key))

(defmethod value-resolver 'spigot/nth
  [[_ value idx] _]
  (nth value idx))

(defmethod value-reducer 'spigot/each
  [[_ expr] values]
  (map (partial resolve-into expr) values))
