(ns org.mongodb.mongo-component
  (:require [com.stuartsierra.component :as component]
            [monger.core :as mg])
  (:import java.lang.IllegalArgumentException))

(defrecord MongoComponent [uri db-name]
  component/Lifecycle
  (start [mongo]
    (if-not (:db mongo)
      (cond
        uri     (let [{:keys [conn db]} (mg/connect-via-uri uri)]
                  (-> "Opening MongoDB connection with uri: %s"
                      (format uri)
                      (println))
                  (assoc mongo :conn conn :db db))

        db-name (let [host (:host mongo)
                      port (Integer. (:port mongo))
                      conn (mg/connect {:host host :port port})
                      db   (mg/get-db conn db-name)]
                  (-> "Opening MongoDB connection with host: %s and port: %d"
                      (format host port)
                      (println))
                  (assoc mongo :conn conn :db db))

        :else   (throw (IllegalArgumentException. (str "Mongo component config "
                                                       "map must have either a "
                                                       "`:uri' or a `:db-name' "
                                                       "key."))))
      mongo))

  (stop [mongo]
    (if (:db mongo)
      (let [conn (:conn mongo)]
        (do (println "Closing MongoDB connection")
            (mg/disconnect conn)
            (dissoc mongo :conn :db)))
      mongo)))

(defn mongo-component [config]
  (map->MongoComponent config))
