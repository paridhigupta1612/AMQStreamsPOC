package org.redhat.kafka.producer;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.nio.file.*;

@RestController
@RequestMapping("/demos/kafka/producer/multithread")
public class KafkaProducerMultithreaded {

	@Value("${bootstrapServer}")
	private String bootstrapServer;

	@PostMapping("/putmessage")
	public String putMessage(@RequestHeader Map<String, String> headers) throws Exception {
		System.out.println("******** KafkaProducerAPI|putMessage|headers=" + headers + "|bootstrapServer="+bootstrapServer);

		try{
			Path path = Paths.get("/deployments", "data1.csv");
			List<String> lines = Files.readAllLines(path);
			System.out.println("Total Records in file = " + lines.size());
			List<String> list1 = lines.subList(1, 10000); //skipped headers
			List<String> list2 = lines.subList(10000, 20000);
			List<String> list3 = lines.subList(20000, lines.size());

			System.out.println("Total Records after Split = " + list1.size()+ ";" + list2.size()+ ";" + list3.size());
			Properties properties = new Properties();
			properties.put("bootstrap.servers",bootstrapServer);
			properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			//properties.put("security.protocol", "SSL");
			//properties.put("ssl.truststore.location", "/deployments/keystore.jks");
			//properties.put("ssl.truststore.password", "password");
			KafkaProducer kafkaProducer = new KafkaProducer<String, String>(properties);

			long startTime = System.currentTimeMillis();
			final ExecutorService executor = Executors.newFixedThreadPool(5);
			final List<Future<?>> futures = new ArrayList<>();
			executor.execute(()->{
				futures.add(executor.submit(new KafkaProducerTask(list1,kafkaProducer)));
				futures.add(executor.submit(new KafkaProducerTask(list2,kafkaProducer)));
				futures.add(executor.submit(new KafkaProducerTask(list3,kafkaProducer)));

				try {
					for (Future<?> future : futures) {
						future.get();
						System.out.println("___________________________________________________KafkaProducer Task completed successfully");
					}
					System.out.println("Total time taken to put records to kafka queue = " + (System.currentTimeMillis() - startTime) + " milliseconds.");
					kafkaProducer.close();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}

			});

		}catch (Exception e){
			e.printStackTrace();
		}
		return "";
	}
}