package org.redhat.kafka.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.redhat.kafka.dto.Customer;


public class KafkaConsumerController {

	public static void getMessage(String bootstrapServer) throws Exception {
		System.out.println("Inside KafkaConsumerController|bootstrapServer="+bootstrapServer);
		Properties properties = new Properties();
		properties.put("bootstrap.servers", bootstrapServer);
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("group.id", "test_group");
		//properties.put("security.protocol", "SSL");
		//properties.put("ssl.truststore.location", "/deployments/keystore.jks");
		//properties.put("ssl.truststore.password", "password");

		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
		kafkaConsumer.subscribe(Collections.singletonList("my-topic"));
		boolean process = Boolean.FALSE;
		System.out.println("************ kafka Consumer **********");
		long startTime = 0;
		List<Customer> successList = null;
		List<Customer> failureList = null;
		try{
			while (true){
				if(!process) {
					startTime = System.currentTimeMillis();		
					successList = new ArrayList<Customer>();
					failureList = new ArrayList<Customer>();
				}
				//process = Boolean.FALSE;
				Duration timeout = Duration.ofMillis(1);
				ConsumerRecords<String, String> records = kafkaConsumer.poll(timeout);
				if(records.count()>0) {
					process = Boolean.TRUE;
					if(!process) {
						startTime = System.currentTimeMillis();
					}
					System.out.println("******** Total records = "+ records.count());					
				}else {
					if(process) {
						System.out.println("*********************************************************** Total time taken = "+ (System.currentTimeMillis()-startTime) +" milliseconds."  + " ***********" + " Successful Records = " + successList.size() + " ***********" + " Failed Records = " + failureList.size());						
					}
					process = Boolean.FALSE;
					startTime = 0;
					successList = null;
					failureList = null;
					continue;
				}				
				//System.out.println("********* Polling queue for messages....");
				for (ConsumerRecord<String, String> record: records){
					System.out.println("***********************************************************"+String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
					//process = Boolean.TRUE;
					Customer customer = KafkaConsumerController.validateRecord(record.value());
					if(customer.isValidationFailed()) {
						failureList.add(customer);	
					}else {
						successList.add(customer);
					}
				}
				//if(!process)continue;

			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}finally {
			kafkaConsumer.close();
		}
	}

	private static Customer validateRecord(String record) {
		String[] p = record.split(",");// a CSV has comma separated lines
		Customer customer = null;
		if(p[0] != null && p[0].trim().length() > 0 && 
				p[1] != null && p[1].trim().length() == 10 &&
				p[2] != null && p[2].trim().length() <= 10 &&
				p[4] != null && p[4].trim().length() == 15) {
			customer = new Customer();
			customer.setName(p[0]);
			customer.setPancardNo(p[1]);
			customer.setDob(p[2]);
			customer.setAmount(Double.valueOf(p[3]));
			customer.setLoanAccNo(p[4]);
			customer.setLastPaymentDate(p[5]);
			customer.setOutstandingAmount(Double.valueOf(p[6]));
			customer.setValidationFailed(Boolean.FALSE);
		}else {
			customer = new Customer();
			customer.setName(p[0]);
			customer.setPancardNo(p[1]);
			customer.setDob(p[2]);
			customer.setAmount(Double.valueOf(p[3]));
			customer.setLoanAccNo(p[4]);
			customer.setLastPaymentDate(p[5]);
			customer.setOutstandingAmount(Double.valueOf(p[6]));
			customer.setValidationFailed(Boolean.TRUE);
		}
		return customer;
	}
}
