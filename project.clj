(defproject introduction-to-langohr "0.1.0-SNAPSHOT"
  :description "An introduction to Langohr"
  :url "https://github.com/chairmanK/introduction-to-langohr.git"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.3.0"]
                 [log4j "1.2.17"]
                 [ring/ring-core "1.3.0"]
                 [ring/ring-jetty-adapter "1.3.0"]
                 [compojure "1.1.8"]
                 [com.novemberain/langohr "2.11.0"]])
