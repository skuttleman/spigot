(ns spigot.controllers.kafka.streams
  (:refer-clojure :exclude [filter map])
  (:import
    (org.apache.kafka.streams KeyValue StreamsBuilder)
    (org.apache.kafka.streams.kstream Aggregator Consumed Grouped Initializer
                                      KeyValueMapper KGroupedStream KStream KTable
                                      Materialized Predicate Produced ValueMapper)))
(defn ^:private ->kv [[k v]]
  (KeyValue. k v))

(deftype FnKeyValueMapper [f]
  KeyValueMapper
  (apply [_ k v]
    (f [k v])))

(deftype FnValueMapper [f]
  ValueMapper
  (apply [_ v]
    (f v)))

(deftype FnInitializer [f]
  Initializer
  (apply [_]
    (f)))

(deftype FnAggregator [reducer]
  Aggregator
  (apply [_ k v agg]
    (reducer agg [k v])))

(deftype FnPredicate [pred]
  Predicate
  (test [_ k v]
    (boolean (pred [k v]))))

(defn ^KStream stream [^StreamsBuilder builder topic-cfg]
  (.stream builder ^String (:name topic-cfg) (Consumed/with (:key-serde topic-cfg) (:val-serde topic-cfg))))

(defn map ^KStream [^KStream stream f]
  (.map stream (->FnKeyValueMapper (comp ->kv f))))

(defn map-values ^KStream [^KStream stream f]
  (.mapValues stream ^ValueMapper (->FnValueMapper f)))

(defn flat-map ^KStream [^KStream stream f]
  (.flatMap stream (->FnKeyValueMapper (comp (partial clojure.core/map ->kv) f))))

(defn filter ^KStream [^KStream stream pred]
  (.filter stream (->FnPredicate pred)))

(defn ^KGroupedStream group-by-key [^KStream stream topic-cfg]
  (.groupByKey stream (Grouped/with (:key-serde topic-cfg) (:val-serde topic-cfg))))

(defn ^KTable aggregate [^KGroupedStream stream init-fn agg-fn topic-cfg]
  (.aggregate stream
              (->FnInitializer init-fn)
              (->FnAggregator agg-fn)
              (Materialized/with (:key-serde topic-cfg) (:val-serde topic-cfg))))

(defn ^Void to [^KStream stream topic-cfg]
  (.to stream ^String (:name topic-cfg) (Produced/with (:key-serde topic-cfg) (:val-serde topic-cfg))))
