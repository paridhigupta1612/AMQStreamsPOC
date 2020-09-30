package org.redhat.kafka;

import org.redhat.kafka.consumer.KafkaConsumerController;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application implements ApplicationRunner{

	@Value("${bootstrapServer}")
    private String bootstrapServer;
	
	public static void main(String[] args) throws Exception{
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		KafkaConsumerController.getMessage(bootstrapServer);
	}
}
