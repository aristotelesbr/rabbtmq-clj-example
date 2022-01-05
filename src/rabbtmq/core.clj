(ns rabbtmq.core
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]
            [langohr.exchange  :as le]))

(def qname
  "test-queeue")
(def ^{:const true} default-exchange
  "")
(def direct-exchange
  "my-direct-exchange")
(def topic-exchange
  "my-topic-exchange")

;; Create a queue
(let [conn (rmq/connect)
      ch   (lch/open conn)]
  (lq/declare ch qname {:exclusive   false
                        :auto-delete false})
  (lch/close ch)
  (rmq/close conn))

;; Create direct exchange
(let [conn (rmq/connect)
      ch   (lch/open conn)]
  (le/declare ch direct-exchange "direct")
  (lq/bind ch qname direct-exchange {:routing-key "direct-route"})
  (lch/close ch)
  (rmq/close conn))

;; Create a topic exchange
(let [conn (rmq/connect)
      ch   (lch/open conn)]
  (le/declare ch topic-exchange "topic")
  (lq/bind ch qname topic-exchange {:routing-key "topic.*"})
  (lch/close ch)
  (rmq/close conn))

;; Publish menssage to exchange
(let [conn (rmq/connect)
      ch   (lch/open conn)]
  (lb/publish ch default-exchange qname "hello")
  (lch/close ch)
  (rmq/close conn))

;; Publish menssage to direct exchange
(let [conn (rmq/connect)
      ch   (lch/open conn)]
  (lb/publish ch direct-exchange "direct-route" "hello from direct")
  (lch/close ch)
  (rmq/close conn))

;; Publish menssage to topic exchange
(let [conn (rmq/connect)
      ch   (lch/open conn)]
  (lb/publish ch topic-exchange "topic.anything" "hello from topic")
  (lch/close ch)
  (rmq/close conn))

;; Consume message from queue
(defn message-handle
  [_chanel _meta payload]
  (let [parsed-payload (String. payload "UTF-8")]
    (println "payload revceived" parsed-payload)))

(let [conn (rmq/connect)
      ch   (lch/open conn)]
  (lc/subscribe ch qname message-handle {:auto-ack true})
  (lch/close ch)
  (rmq/close conn))
