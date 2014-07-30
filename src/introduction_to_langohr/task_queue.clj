(ns introduction-to-langohr.task-queue
  "This is a HTTP service that uses a task queue to perform computation
  tasks asynchronously. Given a collection of numbers `coll`, the
  service computes the sum of its elements, `(apply + coll)`. For the
  sake of demonstration, this computation is performed in a contrived
  and extremely inefficient manner with messaging.

  When the service receives a HTTP request, it enqueues a message on a
  task queue with a unique id, and it returns a HTTP response with this
  id. The payload of the enqueued message is the collection to be
  summed, and the unique id is a message attribute.

  A worker that subscribes to the task queue takes the incoming
  collection, sums its first two elements, and computes a new collection
  that is one element shorter than the original collection. It then
  publishes a message that contains this new collection, with a routing
  key that routes it to the task queue again. The unique id for the
  original request is also attached to the message. Thus, a worker acts
  as both a consumer and a producer, and messages loop around as
  elements in a collection are incrementally summed.

  Finally, when the computation is complete, the result is routed to a
  result queue. The consumer that subscribes to the result queue is
  responsible for storing each result in a data store (this example uses
  the local filesystem) that corresponds to the unique id that was
  assigned at the original HTTP request/response. The service can
  subsequently look up the result by id.

  "
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.tools.cli :as cli]
    [clojure.tools.logging :as log]
    [langohr.core :as rmq]
    [langohr.basic :as lb]
    [langohr.channel :as lch]
    [langohr.confirm :as lcf]
    [langohr.consumers :as lc]
    [langohr.exchange :as le]
    [langohr.queue :as lq]
    [compojure.core :as compojure]
    [ring.adapter.jetty :as jetty]
    [ring.util.response :as ring]))

(defn publish-message-and-confirm
  [ch xname routing-key attributes ^bytes payload]
  (log/debugf
    "Publishing message to exchange \"%s\" with routing key \"%s\""
    xname routing-key)
  (log/debugf
    "Published message has payload %s and attributes: %s"
    (slurp payload) attributes)
  (let [timestamp (java.util.Date.) ; current time
        opts (-> attributes
               (assoc :timestamp timestamp)
               (seq)
               (flatten))]
    (apply lb/publish ch xname routing-key payload opts))
  (lcf/wait-for-confirms ch))

(defn x->bytes
  [x]
  (.getBytes (pr-str x)))

(defn bytes->x
  [^bytes ba]
  (edn/read-string (String. ba)))

(defn +reduction
  "Given a collection, return a sequence with one fewer element than the
  collection. The first element of the output sequence is the sum of the
  first and second elements of the input collection, and the remaining
  elements of the output sequence correspond to the remaining elements
  of the input collection.

  If the collection is empty, return zero. If the collection has exactly
  one element, return that element.

  (+reduction []) => 0
  (+reduction [1]) => 1
  (+reduction [1 2]) => (3)
  (+reduction [1 2 3]) => (3 3)
  (+reduction [1 2 3 4]) => (3 3 4)
  "
  [coll]
  (when-not (coll? coll)
    (throw (ex-info (format "Not a collection: %s" coll) {})))
  (condp = (count coll)
    0 (+)
    1 (first coll)
    (cons (+ (first coll) (second coll)) (nnext coll))))

(defn create-worker
  "Returns a message handler function that performs a computation on an
  incoming task message and publishes an outgoing message. The outgoing
  message may be either another task or a result, and the routing key of
  the outgoing message is set accordingly."
  [exchange task-routing-key result-routing-key]
  (fn [ch {:keys [delivery-tag] :as attributes} ^bytes payload]
    (let [result-or-ex (try
                         (+reduction (bytes->x payload))
                         (catch Exception ex ex))
          as-bytes (x->bytes result-or-ex)]
      (log/debug "Sleeping for 10 seconds to simulate computation time")
      (Thread/sleep 10000)
      (if (coll? result-or-ex)
        (publish-message-and-confirm
          ch exchange task-routing-key attributes as-bytes)
        (publish-message-and-confirm
          ch exchange result-routing-key attributes as-bytes)))
    (lb/ack ch delivery-tag)))

(defn create-archiver
  "Returns a message handler function that extracts a result from an
  incoming result message and stores this result at a location that is
  specified by the `correlation-id` message attribute."
  [path-prefix]
  (fn [ch {:keys [delivery-tag correlation-id] :as attributes} ^bytes payload]
    (let [f (io/file path-prefix correlation-id)]
      (log/debugf
        "Writing result for task id %s to %s"
        correlation-id (.getPath f))
      (with-open [o (io/output-stream f)]
        (.write o payload)))
    (lb/ack ch delivery-tag)))

(defn start-task-queue
  [ch exchange task-routing-key result-routing-key path-prefix]
  (let [worker (create-worker exchange task-routing-key result-routing-key)
        archiver (create-archiver path-prefix)
        conn (rmq/connect)
        ch (doto (lch/open conn) (lcf/select))]
    ; Cleanup
    (.addShutdownHook
      (Runtime/getRuntime)
      (Thread.
        (fn []
          (log/debug "Calling shutdown hook")
          (try (le/delete ch exchange) (catch Exception _ nil))
          (try (rmq/close ch) (catch Exception _ nil))
          (try (rmq/close conn) (catch Exception _ nil)))))
    ; Worker subscribes to task queue
    (le/declare ch exchange "direct")
    (let [{:keys [queue]} (lq/declare ch)]
      (lq/bind ch queue exchange :routing-key task-routing-key)
      (lc/subscribe ch queue worker))
    ; Archiver subscribes to result queue
    (let [{:keys [queue]} (lq/declare ch)]
      (lq/bind ch queue exchange :routing-key result-routing-key)
      (lc/subscribe ch queue archiver))))

(defn submit-task
  [ch exchange task-routing-key http-input]
  (let [payload (.getBytes (slurp http-input))
        id (str (java.util.UUID/randomUUID))
        attributes {:correlation-id id}]
    (publish-message-and-confirm
      ch exchange task-routing-key attributes payload)
    (-> (format "/result/%s" id)
      (ring/response)
      (ring/status 202)))) ; 202 Accepted

(defn get-result
  [path-prefix id]
  (let [f (io/file path-prefix id)]
    (log/debugf "Reading result for task id %s from %s" id (.getPath f))
    (try
      (ring/response (slurp f))
      (catch java.io.FileNotFoundException e
        (-> "Result not found"
          (ring/response)
          (ring/status 404)))))) ; 404 Not Found

(defn start-http
  [ch exchange task-routing-key path-prefix http-port]
  (let [handler (compojure/routes
                  (compojure/POST "/compute" {body :body}
                    (submit-task ch exchange task-routing-key body))
                  (compojure/GET "/result/:id" [id]
                    (get-result path-prefix id)))]
    (jetty/run-jetty handler {:port http-port})
    (log/infof "HTTP server is listening on port %d" http-port)))

(defn start-service
  [path-prefix http-port]
  (let [exchange "exchange"
        task-routing-key "task"
        result-routing-key "result"
        conn (rmq/connect) ; assume default connection settings
        ch (doto (lch/open conn) (lcf/select))]
    ; Automatic cleanup
    (.addShutdownHook
      (Runtime/getRuntime)
      (Thread.
        (fn []
          (log/debug "Calling shutdown hook")
          (try (le/delete ch exchange) (catch Exception _ nil))
          (try (rmq/close ch) (catch Exception _ nil))
          (try (rmq/close conn) (catch Exception _ nil)))))
    (start-task-queue
      ch exchange task-routing-key result-routing-key path-prefix)
    (start-http
      ch exchange task-routing-key path-prefix http-port)))

(def ^:private cli-options
  [["-h" "--help"]
   [nil "--path-prefix PREFIX" "Directory where results will be saved"]
   [nil "--http-port PORT" "HTTP server port"
    :parse-fn #(Long/parseLong %)]])

(defn -main
  [& args]
  (let [{:keys [options summary]} (cli/parse-opts args cli-options)
        tmpdir (System/getProperty "java.io.tmpdir")
        {:keys [help path-prefix http-port]
         :or {path-prefix tmpdir, http-port 8080}} options]
    (when help
      (println summary)
      (System/exit 0))
    (try
      (start-service path-prefix http-port)
      (catch Exception _ (System/exit 1))))
  (System/exit 0))
