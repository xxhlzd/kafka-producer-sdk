package com.allstar.static_server.main;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TestConsumer {
	public static void main(String[] args) {
		consumer();
	}
	
	public static void consumer() {
		Properties prop = new Properties();
		try {
			InputStream in = TestConsumer.class.getResourceAsStream("/consumer.properties");
			prop.load(in);
		} catch ( IOException e) {
			e.printStackTrace();
		}
		Consumer<String, String> cons = new KafkaConsumer<>(prop);
		ArrayList<String> topicList = new ArrayList<String>();
		topicList.add("test");
		cons.subscribe(topicList);
		while (true) {
			ConsumerRecords<String, String> records = cons.poll(100L);
			
			Iterator i = records.iterator();
			if (i.hasNext())
			{
			ConsumerRecord<String, String> record=(ConsumerRecord) i.next();
			System.out.println("topic:" +record.topic() + "value:"+ record.value());
			cons.commitSync();
			}
			
		}
	}
 }
