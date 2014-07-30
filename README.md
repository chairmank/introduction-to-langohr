# Introduction to Langohr

This is a starter project to introduce Clojurians to
[Langohr](https://github.com/michaelklishin/langohr), a Clojure client
for RabbitMQ.

# Requirements

- JDK 1.6 or higher.
- Version 2.0 or higher of [Leiningen](http://leiningen.org/).
- RabbitMQ server. Version 3.0 or higher is recommended. You can
  download and install RabbitMQ server from the official RabbitMQ
  [project page](http://www.rabbitmq.com/).

# Usage

## Hello world

    lein run -m introduction-to-langohr.hello-world

This example demonstrates messaging from a producer to a consumer via
RabbitMQ. Messaging events are carefully logged so that you can watch
the logs and compare with the code to understand what is going on.

## Task queue

    lein run -m introduction-to-langohr.queue

This example is a HTTP service that listens on port 8080 by default.
(The HTTP port can be configured with a command-line option.)

Make a POST request to compute `(apply + [0 1 2 3 4 5])`. The
request body must be serialized as EDN:

    $ curl -i -X POST http://localhost:8080/compute -d '[0 1 2 3 4 5]'
    HTTP/1.1 202 Accepted
    Date: Wed, 30 Jul 2014 14:05:21 GMT
    Content-Length: 44
    Server: Jetty(7.6.13.v20130916)
    
    /result/3e60c036-d510-4f4e-af09-499f5a8a13cc

The service will return this HTTP response immediately. The response
body is the path where the result will become available. Watch the logs
to trace the cascade of messages as the original HTTP request triggers a
pipeline of computation tasks.

Make a GET request with the path that you received in the HTTP response:

    $ curl -i -X GET http://localhost:8080/result/57d36896-dc13-475e-9536-b2ae87b285d7
    HTTP/1.1 404 Not Found
    Date: Wed, 30 Jul 2014 14:07:42 GMT
    Content-Length: 63
    Server: Jetty(7.6.13.v20130916)
    
    Result not found

Oops! The result wasn't ready yet. Try again later:

    $ curl -i -X GET http://localhost:8080/result/57d36896-dc13-475e-9536-b2ae87b285d7
    HTTP/1.1 200 OK
    Date: Wed, 30 Jul 2014 14:07:58 GMT
    Content-Length: 2
    Server: Jetty(7.6.13.v20130916)
    
    15

## Pub-sub

Start publishers:

    lein run -m introduction-to-langohr.pub-sub.publisher

Start 2 subscribers with binding keys `"*.square"` and `"#"`:

    lein run -m introduction-to-langohr.pub-sub.subscriber '*.square' '#'

This example demonstrates messaging from multiple producers to multiple
consumers via a topic exchange. Each consumer creates, binds, and
subscribes to its own queue. Published messages are routed by these
bindings.

# Credits

This project builds upon excellent documentation and code examples for
Langohr that were written by [Michael S.
Klishin](https://github.com/michaelklishin), with contributions from the
rest of the [ClojureWerkz team](http://clojurewerkz.org/) and the open
source community:

- http://clojurerabbitmq.info
- https://github.com/clojurewerkz/langohr.examples

# License

Distributed under the Eclipse Public License, the same as Clojure.
