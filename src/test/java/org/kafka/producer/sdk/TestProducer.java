package com.allstar.static_server.main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;




import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class TestProducer {
	public static void main(String args[]) {
		producer();
	}
	
	public static void producer() {
		Properties props = new Properties();
		try {
			InputStream in =TestProducer.class.getResourceAsStream("/producer.properties");
			props.load(in);
		} catch (FileNotFoundException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        // Hard coding topic too.
        String topic = "test";
        Producer<String, String> pro = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
        	ProducerRecord record = new ProducerRecord(topic, "1", "test message" +i);
	        Future<RecordMetadata> future =pro.send(record);
	        try {
				RecordMetadata meta =  future.get();
				 System.out.println(meta.offset() + " " + meta.partition() + " " + meta.topic());
				 /* 13 */     System.out.println(meta.toString());
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        pro.close();
	}
}
