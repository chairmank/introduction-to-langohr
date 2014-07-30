(ns introduction-to-langohr.pub-sub.subscriber
  "Routing messages from multiple producers to multiple consumers via a
  topic exchange
  (http://clojurerabbitmq.info/articles/exchanges.html#topic-exchanges).

  This namespace contains subscriber functions. Multiple publishers
  (defined in the introduction-to-langohr.pub-sub.publishers namespace)
  send messages with random routing keys to a topic exchange. Consumers
  declare and subscribe queues that receive published messages from the
  topic exchange."
  (:require
    [clojure.tools.cli :as cli]
    [clojure.tools.logging :as log]
    [langohr.core :as rmq]
    [langohr.basic :as lb]
    [langohr.channel :as lch]
    [langohr.confirm :as lcf]
    [langohr.consumers :as lc]
    [langohr.exchange :as le]
    [langohr.queue :as lq]))

(defn log-and-acknowledge
  [subscriber-id ch attributes]
  (let [{:keys [app-id routing-key delivery-tag]} attributes]
    (log/debugf
      "Subscriber %s received message from publisher %s: %s"
      subscriber-id app-id routing-key)
    (lb/ack ch delivery-tag)))

(defn start-subscriber
  "Declare a queue, bind the queue to the exchange, and consume from the
  queue. Log each received message."
  [id conn exchange binding-key]
  (log/debugf
    "Starting subscriber %s with binding-key %s" id binding-key)
  (let [ch (doto (lch/open conn) (lcf/select))
        {:keys [queue]} (lq/declare ch) ; server-named queue
        message-handler (fn [ch attributes _]
                          (log-and-acknowledge id ch attributes))]
    (lq/bind ch queue exchange :routing-key binding-key)
    (lc/subscribe ch queue message-handler)))

(defn start-subscribers
  [exchange binding-keys]
  (let [conn (rmq/connect)
        ch (doto (lch/open conn) (lcf/select))]
    (le/declare ch exchange "topic")
    (doseq [[i k] (map-indexed vector binding-keys)]
      (start-subscriber (str i) conn exchange k))))

(def ^:private cli-options
  [["-h" "--help"]
   [nil "--exchange EXCHANGE" "Exchange to publish messages to"]])

(defn -main
  [& args]
  (let [{:keys [options arguments summary]} (cli/parse-opts args cli-options)
        {:keys [help exchange] :or {exchange "pub-sub"}} options]
    (when help
      (println summary)
      (System/exit 0))
    (start-subscribers exchange arguments)
    ; Block forever
    (loop [] (Thread/sleep 10000) (recur)))
  (System/exit 0))
