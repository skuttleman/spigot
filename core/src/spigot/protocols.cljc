(ns spigot.protocols
  (:refer-clojure :exclude [next resolve]))

(defprotocol ICtxResolver
  (resolve [this ctx]))
