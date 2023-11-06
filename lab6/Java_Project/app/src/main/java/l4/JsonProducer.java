package l4;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonProducer {

	private static final Logger LOG = LoggerFactory.getLogger(JsonProducer.class);

	private static final String BOOTSTRAP_SERVERS = ":9092";
	private static final String CLIENT_ID = "ex";

	private static Producer<String, String> producer;

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		producer = new KafkaProducer<>(props);

		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		//newSingleThreadScheduledExecutor --> Creates an Executor that uses a single worker thread operating off an unbounded queue.
		executor.scheduleAtFixedRate(() -> send("events2"), 0, 3, TimeUnit.SECONDS);

		Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
	}



	public static void send(String topic) {
        String filePath = "D:\\Alex\\Facultate\\SE an 1\\MQT\\Labs\\L4\\app\\src\\main\\java\\l4\\test.json"; // Replace with the actual file path

        try {
            StringBuilder jsonContent = new StringBuilder();
            String line;
            BufferedReader reader = new BufferedReader(new FileReader(filePath));

            while ((line = reader.readLine()) != null) {
                jsonContent.append(line);
            }

            reader.close();

            String jsonString = jsonContent.toString();

            final int number = new Random().nextInt(10);
            ProducerRecord<String, String> data = new ProducerRecord<>(topic, "key"+number, jsonString);
            try {
                RecordMetadata meta = producer.send(data).get();
                LOG.info("key = {}, value = {} ==> partition = {}, offset = {}", data.key(), data.value(), meta.partition(), meta.offset());
            }catch (InterruptedException | ExecutionException e){
                producer.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
		// final int number = new Random().nextInt(10);
		// ProducerRecord<String, String> data = new ProducerRecord<>(topic, "key" + number, "v"+number);
		// try {
		// 	RecordMetadata meta = producer.send(data).get();
		// 	System.out.println(String.format("----------------- Example Producer -------------- key = %s, value = %s => partition = %d, offset= %d", data.key(), data.value(), meta.partition(), meta.offset()));
		// 	//LOG.info("----------------- Example Producer -------------- key = {}, value = {} => partition = {}, offset= {}", data.key(), data.value(), meta.partition(), meta.offset());
		// } catch (InterruptedException | ExecutionException e) {
		// 	producer.flush();
		// }
	}

}
