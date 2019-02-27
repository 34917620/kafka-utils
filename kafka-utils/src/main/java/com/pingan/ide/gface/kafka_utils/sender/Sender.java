package com.pingan.ide.gface.kafka_utils.sender;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pingan.ide.gface.kafka_utils.factory.KafkaFactroy;

public class Sender {

	private static final Logger logger = LoggerFactory.getLogger(Sender.class);
	
	private static Producer<String,String> producer = KafkaFactroy.getProducer();
	
	public static boolean send(String topic,String msg){
        boolean flag = true;
		try {
			RecordMetadata recordMetadata= producer.send(new ProducerRecord<String, String>(topic,msg)).get();
		    logger.debug(recordMetadata.toString());
		} catch (InterruptedException e) {
			logger.error(e.getMessage(),e);
			flag = false;
		} catch (ExecutionException e) {
			logger.error(e.getMessage(),e);
			flag = false;
		}
	    return flag;
	}
	
	public static boolean send(String topic, Integer partition, String key, String value){
		 boolean flag = true;
			try {
				RecordMetadata recordMetadata= producer.send(new ProducerRecord<String, String>(topic,partition,key,value)).get();
			    logger.debug(recordMetadata.toString());
			} catch (InterruptedException e) {
				logger.error(e.getMessage(),e);
				flag = false;
			} catch (ExecutionException e) {
				logger.error(e.getMessage(),e);
				flag = false;
			}
		    return flag;
	}
	
	public static boolean send(String topic, Integer partition, Long timestamp, String key, String value){
		 boolean flag = true;
			try {
				RecordMetadata recordMetadata= producer.send(new ProducerRecord<String, String>(topic, partition,timestamp,key,value)).get();
			    logger.debug(recordMetadata.toString());
			} catch (InterruptedException e) {
				logger.error(e.getMessage(),e);
				flag = false;
			} catch (ExecutionException e) {
				logger.error(e.getMessage(),e);
				flag = false;
			}
		    return flag;
	}
	
	public static List<PartitionInfo> getPartition(String topic){
		return producer.partitionsFor(topic);
	}

	public static void metrics(){
		Map<MetricName, ? extends Metric> map =producer.metrics();
		for(Iterator<MetricName> it= map.keySet().iterator();it.hasNext();){
			MetricName key = it.next();
			System.out.println(key.toString()+"---"+map.get(key).toString());
		}
	}
}
