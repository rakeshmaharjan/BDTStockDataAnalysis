package StockSparkKafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class StockKafkaConsumer {

	private KafkaConsumer<String, String> consumer;
	
	private String recentKey = "";
	public String TOPIC = "stock-topic";
	public String PORT = "9092";

	public StockKafkaConsumer() {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:" + this.PORT);
		props.put("group.id", "stock-group");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		this.consumer = new KafkaConsumer<String, String>(props);
		this.consumer.subscribe(Arrays.asList(this.TOPIC));

	}

	public void Delay(Predict predictable, ConsumerCallback callback) {
		while (predictable.check()) {
			@SuppressWarnings("deprecation")
			ConsumerRecords<String, String> records = this.consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				callback.consume(record.key(), record.value(),
						this.recentKey.compareTo(record.key().toLowerCase()) != 0);
				this.recentKey = record.key().toLowerCase();
			}
		}
	}

	@FunctionalInterface
	public interface Predict {
		boolean check();
	}

	@FunctionalInterface
	public interface ConsumerCallback {
		void consume(String key, String val, boolean isNew);
	}

}
