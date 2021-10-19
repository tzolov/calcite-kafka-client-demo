package com.example.calcitekafkaclientdemo;

import java.net.URI;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class CloudEventsProducerApplication {

	public static final int MESSAGE_COUNT = 1000;
	public static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";
	public static final String KAFKA_TOPIC = "my-cloud-events";
	private static final Random random = new Random();

	public static void main(String[] args) {

		// Basic producer configuration
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-cloudevents-producer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		// Configure the CloudEventSerializer to emit events as json structured events
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
		props.put(CloudEventSerializer.ENCODING_CONFIG, Encoding.STRUCTURED);
		props.put(CloudEventSerializer.EVENT_FORMAT_CONFIG, JsonFormat.CONTENT_TYPE);

		// Create the KafkaProducer
		KafkaProducer<String, CloudEvent> producer = new KafkaProducer<>(props);

		// Create an event template to set basic CloudEvent attributes
		CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
				.withSource(URI.create("https://github.com/cloudevents/sdk-java/tree/master/examples/kafka"))
				.withType("producer.example");

		for (int i = 0; i < MESSAGE_COUNT; i++) {
			try {
				String id = UUID.randomUUID().toString();
				String data = "Event number " + i;

				// Create the event starting from the template
				CloudEvent event = eventTemplate.newBuilder()
						.withId(id)
						.withData("text/plain", data.getBytes())
						.build();

				// Send the record
				RecordMetadata metadata = producer.send(new ProducerRecord<>(KAFKA_TOPIC, id, event)).get();

				Thread.sleep(50 * random.nextInt(5));

				System.out.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset());
			}
			catch (Exception e) {
				System.out.println("Error while trying to send the record");
				e.printStackTrace();
				return;
			}
		}

		// Flush and close the producer
		producer.flush();
		producer.close();
	}
}
