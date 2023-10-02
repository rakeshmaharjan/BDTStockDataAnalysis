package StockSparkKafka;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
public class App {

	static String recentKey = "";
	static List<String> list = new ArrayList<String>();

	public static void main(String[] args) throws Exception {
		
		SparkContexts spark = new SparkContexts();
		
		StockKafkaConsumer stockKafkaConsumer = new StockKafkaConsumer();
		stockKafkaConsumer.Delay(() -> {
			try {
				Thread.sleep(3000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return true;
		}, (String key, String val, boolean isNew) -> {
			if (recentKey .isEmpty())
				recentKey  = key;
			if (isNew) {
				try {
					if (!list.isEmpty())
						spark.TaskHandler(recentKey, list);
				} catch (Exception e) {
					e.printStackTrace();
				}

				recentKey = key;
				list.clear();
			}
				list.add(key);
			});
	}
}