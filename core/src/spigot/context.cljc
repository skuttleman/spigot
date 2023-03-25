(ns spigot.context
  (:refer-clojure :exclude [resolve])
  (:require
    [clojure.walk :as walk]
    [spigot.protocols :as psp])
  #?(:clj
     (:import
       (clojure.lang PersistentList))))

(defmulti ^:private resolve-list
          (fn [[tag] _]
            tag))

(defmethod resolve-list :default
  [list _]
  list)

(extend-protocol psp/ICtxResolver
  #?(:cljs List :default PersistentList)
  (resolve [this ctx]
    (resolve-list this ctx)))

(defn resolve-params [params ctx]
  (when params
    (walk/postwalk (fn [x]
                     (cond-> x
                       (satisfies? psp/ICtxResolver x) (psp/resolve ctx)))
                   params)))

(defn merge-ctx [ctx ->ctx result]
  (reduce (fn [ctx [param k]]
            (if-let [[_ v] (find result k)]
              (assoc ctx param v)
              ctx))
          ctx
          ->ctx))

(defmethod resolve-list 'spigot/get
  [[_ key] ctx]
  (get ctx key))
