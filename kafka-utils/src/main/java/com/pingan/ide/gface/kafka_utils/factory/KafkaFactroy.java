package com.pingan.ide.gface.kafka_utils.factory;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import com.pingan.ide.gface.kafka_utils.config.KafkaConfig;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * kafka工厂，主要用来生成消息者和生产者
 * @author wangzhi802
 *
 */
public class KafkaFactroy {

	private static Producer<String,String> producer = null;
	
	private synchronized static void initProducer(){
		producer = new KafkaProducer<String, String>(KafkaConfig.getProducerConfig());
		
	}
	
	/**
	 * 获取消息连接器
	 * @param consumerGroup 消息者分组
	 * @return 连接器
	 */
	public static ConsumerConnector getConsumerConnector(String consumerGroup){
		Properties props= KafkaConfig.getConsumerConfig();
		props.put("group.id", consumerGroup);
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector cc = Consumer.createJavaConsumerConnector(config);
		return cc;
	}
	
	/**
	 * 获取生产者实例
	 * @return
	 */
	public static Producer<String,String> getProducer(){
		if(producer==null){
			initProducer();
		}
		return producer;
	}
}
