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

(defn ^:private resolve-params [params ctx]
  (when params
    (walk/postwalk (fn [x]
                     (cond-> x
                       (list? x) (-> (spm/resolve-param ctx))))
                   params)))

(defn resolve-with-sub-ctx [params ctx opts]
  (let [sub-ctx (resolve-params (:spigot/ctx opts) ctx)]
    (-> params
        (resolve-params (merge ctx sub-ctx)))))

(defn merge-ctx [ctx mapping result opts]
  (into ctx
        (map (fn [[->ctx result->]]
               [(resolve-with-sub-ctx ->ctx ctx opts)
                (resolve-with-sub-ctx result-> result opts)]))
        mapping))
