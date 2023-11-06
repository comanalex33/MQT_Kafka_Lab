package l4;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePipeline {

	private static final Logger LOG = LoggerFactory.getLogger(App.class);

	private static final String BOOTSTRAP_SERVERS = ":9092";
	private static final String GROUP_ID = "ex";
	private static final String OFFSET_RESET = "earliest";

	private static final String CLIENT_ID = "ex";
	private static Producer<String, String> producer;

	@SuppressWarnings("boxing")
	public static void main(String[] args) {

		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		producer = new KafkaProducer<>(producerProps);

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
			consumer.subscribe(Collections.singleton("events2"), new ConsumerRebalanceListener() {

				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					LOG.info("onPartitionsRevoked - partitions:{}", formatPartitions(partitions));
				}

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					LOG.info("onPartitionsAssigned - partitions: {}", formatPartitions(partitions));

				}
			});
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
				for (ConsumerRecord<String, String> data : records) {
					LOG.info("--------------  Consumer Part ----------- topic = {}, key = {}, value = {} => partition = {}, offset= {}", "events2", data.key(), data.value(), data.partition(), data.offset());
					
					// Filter data
					if (data.value().equals("v7")) {
						send("events1", data.key(), data.value());
					}
					
				}
			}
		} catch (Exception e) {
			LOG.error("Something goes wrong: {}", e.getMessage(), e);
		}
	}

	@SuppressWarnings("boxing")
	public static String formatPartitions(Collection<TopicPartition> partitions) {
		return partitions.stream()
				.map(topicPartition -> String.format("topic: %s, partition: %s", topicPartition.topic(), topicPartition.partition()))
				.collect(Collectors.joining(", ", "[", "]"));
	}

	public static void send(String topic, String key, String value) {
		ProducerRecord<String, String> data = new ProducerRecord<>(topic, key, value);
		try {
			RecordMetadata meta = producer.send(data).get();
			System.out.println(String.format("----------------- Producer part -------------- topic = %s, key = %s, value = %s => partition = %d, offset= %d", topic, data.key(), data.value(), meta.partition(), meta.offset()));
		} catch (InterruptedException | ExecutionException e) {
			producer.flush();
		}
	}
}
