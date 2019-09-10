package com.pharbers.ipaas.data.driver.libs.kafka

import java.util.concurrent.TimeUnit

import com.pharbers.kafka.producer.PharbersKafkaProducer
import org.apache.avro.specific.SpecificRecord

case class ProducerAvroTopic(topic: String, record: SpecificRecord) {
	val pkp = new PharbersKafkaProducer[String, SpecificRecord]
	val fu = pkp.produce(topic, "value", record)
	println(fu.get(10, TimeUnit.SECONDS))
}
