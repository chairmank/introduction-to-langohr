(ns introduction-to-langohr.pub-sub.publisher
  "Routing messages from multiple producers to multiple consumers via a
  topic exchange
  (http://clojurerabbitmq.info/articles/exchanges.html#topic-exchanges).

  This namespace contains publisher functions. Multiple publishers send
  messages with random routing keys to a topic exchange. Published
  messages are routed to queues that are declared and subscribed by
  consumers defined in the introduction-to-langohr.pub-sub.subscriber
  namespace."
  (:require
    [clojure.tools.cli :as cli]
    [clojure.tools.logging :as log]
    [langohr.core :as rmq]
    [langohr.basic :as lb]
    [langohr.channel :as lch]
    [langohr.confirm :as lcf]
    [langohr.exchange :as le]))

(def colors
  ["cyan" "yellow" "magenta"])

(def shapes
  ["circle" "triangle" "square"])

(defn random-routing-key
  "Generate a random routing key of the form {color}.{shape}"
  []
  (format "%s.%s" (rand-nth colors) (rand-nth shapes)))

(defn publish-message-and-confirm
  [ch xname routing-key attributes ^bytes payload]
  (log/debugf
    "Publishing message to exchange \"%s\" with routing key \"%s\""
    xname routing-key)
  (let [timestamp (java.util.Date.) ; current time
        opts (-> attributes
               (assoc :timestamp timestamp)
               (seq)
               (flatten))]
    (apply lb/publish ch xname routing-key payload opts))
  (lcf/wait-for-confirms ch))

(defn start-publisher
  "Create a future that loops infinitely and never completes. On each
  loop cycle, the thread sleeps and then a message is published to the
  exchange with an empty (zero-byte) payload and the :app-id attribute
  set to the identify of this publisher."
  [id conn exchange interval]
  (let [ch (doto (lch/open conn) (lcf/select))]
    (future
      (loop []
        (Thread/sleep interval)
        (publish-message-and-confirm
          ch exchange (random-routing-key) {:app-id id} (byte-array 0))
        (recur)))))

(defn start-publishers
  [num-publishers exchange interval]
  (let [conn (rmq/connect)
        ch (doto (lch/open conn) (lcf/select))]
    (le/declare ch exchange "topic")
    (for [i (range num-publishers)]
      (start-publisher (str i) conn exchange interval))))

(def ^:private cli-options
  [["-h" "--help"]
   [nil "--num-publishers N" "Number of publishers"]
   [nil "--exchange EXCHANGE" "Exchange to publish messages to"]
   [nil "--interval INTERVAL" "Approximate interval between messages (ms)"
    :parse-fn #(Long/parseLong %)]])

(defn -main
  [& args]
  (let [{:keys [options summary]} (cli/parse-opts args cli-options)
        {:keys [help exchange interval num-publishers]
         :or {exchange "pub-sub"
              interval 1000
              num-publishers 3}} options]
    (when help
      (println summary)
      (System/exit 0))
    ; start-publishers returns a sequence of futures that block
    ; forever
    (->> (start-publishers num-publishers exchange interval)
      (map deref)
      (doall)))
  (System/exit 0))
