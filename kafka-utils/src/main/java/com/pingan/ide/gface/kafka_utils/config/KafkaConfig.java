package com.pingan.ide.gface.kafka_utils.config;
/**
 * 加载配置文件
 * @author wangzhi802
 *
 */

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfig {
	private static final String F_CONSUMER_PROP_FILE="kafka-consumer.properties";  //消费者配置
	private static final String F_PRODUCER_PROP_FILE="kafka-producer.properties";  //生产者配置
	
	private static final String CONSUMER_PROP_FILE="META-INFO/kafka-consumer.properties";  //消费者配置
	private static final String PRODUCER_PROP_FILE="META-INFO/kafka-producer.properties";  //生产者配置
	private static Properties consumerConfig = null;
	private static Properties producerConfig = null;
	
	private synchronized static void loadConsumerConfig(){
		InputStream in=null;
		try {
			in = KafkaConfig.class.getClassLoader().getResourceAsStream(F_CONSUMER_PROP_FILE);
			if(in==null){
				in = KafkaConfig.class.getClassLoader().getResourceAsStream(CONSUMER_PROP_FILE);
			}
			Properties props = new Properties();
			props.load(in);
			consumerConfig = props;
			in.close();
			in=null;
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(in!=null){
			   try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			}
		}
	}
	
	private synchronized static void loadProducerConfig(){
		InputStream in=null;
		try {
			in = KafkaConfig.class.getClassLoader().getResourceAsStream(F_PRODUCER_PROP_FILE);
			if(in==null){
				in = KafkaConfig.class.getClassLoader().getResourceAsStream(PRODUCER_PROP_FILE);
			}
			Properties props = new Properties();
			props.load(in);
			producerConfig = props;
			in.close();
			in=null;
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(in!=null){
			   try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			}
		}
	}
	
	public static Properties getConsumerConfig(){
		if(consumerConfig==null){
			loadConsumerConfig();
		}
		return (Properties) consumerConfig.clone();
	}
	
	public static Properties getProducerConfig(){
		if(producerConfig==null){
			loadProducerConfig();
		}
		return (Properties) producerConfig.clone();
	}

}
