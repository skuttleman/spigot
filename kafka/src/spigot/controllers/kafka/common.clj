(ns spigot.controllers.kafka.common
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io])
  (:import
    (java.io PushbackReader)
    (java.util Properties)
    (org.apache.kafka.common.serialization Serde Serializer Deserializer)
    (org.apache.kafka.streams KeyValue)))

(deftype SpigotSerde [^Serializer serializer ^Deserializer deserializer]
  Serde
  (serializer [_]
    serializer)
  (deserializer [_]
    deserializer))

(deftype EdnSerializer []
  Serializer
  (serialize [_ _ payload]
    (-> payload pr-str .getBytes)))

(deftype EdnDeserializer []
  Deserializer
  (deserialize [_ _ payload]
    (-> payload io/input-stream io/reader PushbackReader. edn/read)))

(defn create-serde [^Serializer serializer ^Deserializer deserializer]
  (->SpigotSerde serializer deserializer))

(defn default-serde []
  (create-serde (->EdnSerializer) (->EdnDeserializer)))

(defn ->props ^Properties [m]
  (let [props (Properties.)]
    (doseq [kv m
            :let [k (key kv)
                  v (val kv)]]
      (.put props (name k) (cond-> v (keyword? v) name)))
    props))

(defn ->topic-cfg [topic]
  {:name topic
   :key-serde (default-serde)
   :val-serde (default-serde)})

(defn ->kv-pair [^KeyValue kv]
  [(.-key kv) (.-value kv)])
