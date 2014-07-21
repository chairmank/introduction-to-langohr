(ns introduction-to-langohr.hello-world
  "Messaging from a producer to a consumer via a direct exchange
  (http://clojurerabbitmq.info/articles/exchanges.html#direct-exchanges).

  Setup
  -----
  This example has one exchange (of \"direct\" type) and one queue. The
  queue is bound to the exchange. The consumer subscribes to the queue.

  Producer
  --------
  The producer publishes to the exchange, and it uses publisher
  confirmation
  (http://clojurerabbitmq.info/articles/exchanges.html#using-the-publisher-confirms-extension)
  to confirm with the broker that the message was published.

  Consumer
  --------
  The consumer receives the message. When the message is first received,
  its `redelivered?` attribute is false. The consumer has a message
  handler that sends the broker a negative acknowledgement, or \"nack\"
  (http://clojurerabbitmq.info/articles/queues.html#negative-acknowledgements)
  when the `redelivered?` attribute of the message is false. The message
  handler sets an option to requeue the nack'ed message. The message
  returns to the same queue on the broker.

  When the message is eventually redelivered from the queue, the message
  handler sees that the `redelivered?` attribute is true. It sends
  the broker an acknowledgement
  (http://clojurerabbitmq.info/articles/queues.html#message-acknowledgements),
  which tells the broker that the message is in final custody of the
  consumer, so it can be deleted from the queue.
  "
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

(defn open-connection
  [settings]
  (log/debugf
    "Opening connection to RabbitMQ broker with settings: %s"
    settings)
  (rmq/connect settings))

(defn close-connection
  [conn]
  (log/debugf "Closing connection to RabbitMQ broker: %s" conn)
  (rmq/close conn))

(defn open-channel
  [conn]
  (let [ch (lch/open conn)
        n (.getChannelNumber ch)]
    (log/debugf "Opened channel number %d" n)
    (log/debugf "Putting channel %d into confirmation mode" n)
    (lcf/select ch)
    ch))

(defn close-channel
  [ch]
  (log/debugf "Closing channel number %d" (.getChannelNumber ch))
  (rmq/close ch))

(defn declare-exchange
  "Declare a durable exchange. This function is idempotent. If the
  exchange does not exist, then it is created."
  [ch xname xtype]
  (log/debugf "Declaring exchange \"%s\" of type \"%s\"" xname xtype)
  (le/declare ch xname xtype :durable true))

(defn delete-exchange
  [ch xname]
  (log/debugf
    "Deleting exchange \"%s\" and unbinding all queues that are bound to it"
    xname)
  (le/delete ch xname))

(defn declare-queue
  "Declare a durable queue. This function is idempotent. If the exchange
  does not exist, then it is created."
  [ch qname]
  (log/debugf "Declaring queue \"%s\"" qname)
  (lq/declare ch qname :durable true))

(defn delete-queue
  [ch qname]
  (log/debugf "Deleting queue \"%s\"" qname)
  (lq/delete ch qname))

(defn bind
  "Bind a queue to an exchange"
  [ch qname xname routing-key]
  (log/debugf
    "Binding queue \"%s\" to exchange \"%s\" with routing-key key \"%s\""
    qname xname routing-key)
  (lq/bind ch qname xname :routing-key routing-key))

(defn bytes->string
  [^bytes b]
  (String. b "UTF-8"))

(defn string->bytes
  [^String s]
  (.getBytes s "UTF-8"))

(defn subscribe
  [ch qname message-handler]
  (log/debugf "Subscribing to queue \"%s\"" qname)
  (lc/subscribe ch qname message-handler))

(defn publish-message-and-confirm
  [ch xname routing-key attributes ^bytes payload]
  (log/debugf
    "Publishing message to exchange \"%s\" with routing key \"%s\""
    xname routing-key)
  (log/debugf
    "Published message has payload of %d bytes and attributes: %s"
    (alength payload) attributes)
  (let [timestamp (java.util.Date.) ; current time
        opts (-> attributes
               (assoc :persistent true)
               (assoc :timestamp timestamp)
               (seq)
               (flatten))]
    (apply lb/publish ch xname routing-key payload opts))
  (lcf/wait-for-confirms ch)
  (log/debug "Published message was confirmed by broker"))

(defn handle-message
  "Handle a received message. If the `redelivery?` attribute is false,
  nack the message, requeue it, and return false. If the `redelivery?`
  attribute is true, ack the message, and return true."
  [ch {:keys [delivery-tag redelivery?] :as attributes} ^bytes payload]
  (log/debugf
    "Consumer received message with payload of %d bytes and attributes: %s"
    (alength payload) attributes)
  (log/debugf
    "Message payload as UTF-8 string: \"%s\"" (String. payload "UTF-8"))
  (if redelivery?
    (do
      (log/debug "This is a redelivered message; acking.")
      (lb/ack ch delivery-tag))
    (do
      (log/debug
        "This message has not been delivered before; nacking with requeue.")
      (lb/nack ch delivery-tag false true)))
  redelivery?)

(defn hello-world
  [rabbitmq-config]
  (let [queue "queue"
        exchange "exchange"
        routing-key "hello"
        conn (open-connection rabbitmq-config)
        ch (open-channel conn)
        acknowledged-payload (atom nil)
        message-handler (fn [ch attributes payload]
                          (when (handle-message ch attributes payload)
                            (reset! acknowledged-payload payload)))]
    ; Setup
    (declare-exchange ch exchange "direct")
    (declare-queue ch queue)
    (bind ch queue exchange routing-key)
    ; Consumer subscribes to queue
    (subscribe ch queue message-handler)
    ; Producer publishes message to exchange with routing key
    (let [payload (string->bytes "Hello world!")
          attributes {:content-type "text/plain"}]
      (publish-message-and-confirm ch exchange routing-key attributes payload))
    ; Wait for consumer to receive and acknowledge the message
    (while (nil? @acknowledged-payload)
      (Thread/sleep 1000))
    (log/debugf
      "Consumer acknowledged message with payload: \"%s\""
      (bytes->string @acknowledged-payload))
    ; Query status of the queue
    (log/debugf "Status of queue \"%s\": %s" queue (lq/status ch queue))
    ; Cleanup
    (delete-exchange ch exchange)
    (delete-queue ch queue)
    (close-channel ch)
    (close-connection conn)))

(def ^:private cli-options
  [["-h" "--help"]
   [nil "--host HOST" "Hostname on which the RabbitMQ server runs"]
   [nil "--port PORT" "RabbitMQ server port"]
   [nil "--vhost VHOST" "Virtual host"]])

(defn -main
  [& args]
  (let [{:keys [options summary]} (cli/parse-opts args cli-options)
        {:keys [help host port vhost]} options
        rabbitmq-config (-> rmq/*default-config*
                          (merge (when host {:host host}))
                          (merge (when port {:port port}))
                          (merge (when vhost {:vhost vhost})))]
    (when help
      (println summary)
      (System/exit 0))
    (try
      (hello-world rabbitmq-config)
      (catch Exception e
        (log/error e e)
        (System/exit 1)))
    (System/exit 0)))
