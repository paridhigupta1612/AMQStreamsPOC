package org.redhat.kafka.producer;

import java.util.List;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerTask implements Runnable {
	
	private List<String> lines;
	private KafkaProducer<String, String> kafkaProducer;

	public KafkaProducerTask(List<String> lines, KafkaProducer<String, String> kafkaProducer) {
		this.lines = lines;
		this.kafkaProducer = kafkaProducer;
	}

	@Override
	public void run() {
		System.out.println(String.format("starting task thread %s", Thread.currentThread().getName()));
		try {
			for (final String line : lines) {
				//System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++  "+line);
				kafkaProducer.send(new ProducerRecord<String, String>("my-topic", line));
				//System.out.println("___________________________________________________");
			}
			System.out.println(String.format("Completed task thread %s", Thread.currentThread().getName()));
		}
		catch(Exception e) {
			e.printStackTrace();
			System.out.println("******************************"+e);
		}
	}
}
