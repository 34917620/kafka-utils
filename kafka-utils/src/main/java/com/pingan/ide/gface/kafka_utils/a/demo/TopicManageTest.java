package com.pingan.ide.gface.kafka_utils.a.demo;

import java.util.Map;

import com.pingan.ide.gface.kafka_utils.admin.TopicManage;

import scala.collection.Seq;


public class TopicManageTest {
	public static void main(String[] args) {
		//TopicManage.createTopic("t3", 2, 3);
		//TopicManage.deleteTopic("t3");
		//System.out.println(TopicManage.getAllConsumerGroupsForTopic("my-replicated-topic"));
		//Properties prop = TopicManage.fetchEntityConfig("my-replicated-topic");
		
		Map map = TopicManage.getPartitionsForTopics("my-replicated-topic");
		Seq<Object> seq = (Seq<Object>) TopicManage.getPartitionsForTopics("my-replicated-topic").get("my-replicated-topic");
		System.out.println(TopicManage.getAllPartitions());
		
	}
}
