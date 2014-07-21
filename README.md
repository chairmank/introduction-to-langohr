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

    lein run -m introduction-to-langohr.hello-world

This example demonstrates messaging from a producer to a consumer via
RabbitMQ. Messaging events are carefully logged so that you can watch
the logs and compare with the code to understand what is going on.

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
