(ns rabbtmq.core
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]))

(def qname
  "test-queeue")

(def ^{:const true} default-exchange
  "")

;; Create a queue
(let [conn (rmq/connect)
      ch   (lch/open conn)]
  (lq/declare ch qname {:exclusive   false
                        :auto-delete false})
  (lch/close ch)
  (rmq/close conn))

;; Publish menssage to exchange
(let [conn (rmq/connect)
      ch   (lch/open conn)]
  (lb/publish ch default-exchange qname "hello")
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
