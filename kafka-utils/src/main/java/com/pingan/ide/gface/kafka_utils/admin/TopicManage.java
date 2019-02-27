package com.pingan.ide.gface.kafka_utils.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.security.JaasUtils;

import com.pingan.ide.gface.kafka_utils.config.KafkaConfig;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.common.TopicAndPartition;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;


/**
 * topic管理工具类
 * @author wangzhi802
 *
 */
public class TopicManage {

	private static final int PARTITIONS = 3;
	private static final int REPLICAS = 2;
	
	/**
	 * 创建topic
	 * @param topic
	 * @param partitions 分区数
	 * @param replicas  副本数，ps:不能大于broker的总数
	 */
	public static void createTopic(String topic,int partitions,int replicas){
		ZkUtils zkUtils = getZkUtils();
		AdminUtils.createTopic(zkUtils,topic, partitions, replicas, new Properties(), RackAwareMode.Enforced$.MODULE$);
		zkUtils.close();
	}
	
	   /**
     * -根据brokerId获取broker的信息
     * @param brokerId
     */
    public static Broker getBrokerInfo(int brokerId){
    	ZkUtils zkUtils = getZkUtils();
    	Broker broker = zkUtils.getBrokerInfo(brokerId).get();
    	zkUtils.close();
        return broker;
    }

	
	/**
	 * 使用默认的分区数与副本数来创建一个topic 
	 * @param topic
	 */
	public static void createTopic(String topic){
		createTopic(topic,PARTITIONS,REPLICAS);
	}
	
	/**
	 * 删除topic, 需要kafka服务器上设置允许删除才能起作用
	 * @param topic
	 */
	public static void deleteTopic(String topic){
		ZkUtils zkUtils = getZkUtils();
		AdminUtils.deleteTopic(zkUtils,topic);
		zkUtils.close();
	}
	/**
	 * 增加分区
	 * @param topic 主题
	 * @param cnt  增加到的分区数--增加后分区总数。 只能比原来分区数大，不能比原来的小
	 */
	public static void addPartitions(String topic,int cnt){
		ZkUtils zkUtils = getZkUtils();
		AdminUtils.addPartitions(zkUtils, topic, cnt, "", true, RackAwareMode.Enforced$.MODULE$);
		zkUtils.close();
	}
	
	 /**
     * 判断某个topic是否存在
     * @param topicName
     * @return
     */
    public static boolean topicExists(String topicName) {
    	ZkUtils zkUtils = getZkUtils();
    	boolean flag = AdminUtils.topicExists(zkUtils, topicName);
    	zkUtils.close();
        return flag;
    }
    
    /**
     * ~获取某个分组下的所有消费者
     * @param groupName
     * @return
     */
    public static  List<String> getConsumersInGroup(String groupName) {
    	ZkUtils zkUtils = getZkUtils();
        List<String> allConsumersList = JavaConversions.seqAsJavaList(zkUtils.getConsumersInGroup(groupName));
        zkUtils.close();
        return allConsumersList;
    }
    /**
     * -获取集群信息（与getAllBrokersInCluster（）只是返回类型不一致）
     */
    public static Cluster getCluster(){
    	ZkUtils zkUtils = getZkUtils();
    	Cluster cluster = zkUtils.getCluster();
    	zkUtils.close();
        return cluster;
    }

    /**
     * 获取所有的TopicList
     * @return
     */
	public static List<String> getTopicList() {
		ZkUtils zkUtils = getZkUtils();
	    List<String> allTopicList = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
	    zkUtils.close();
	    return allTopicList;
	}

	 /**
     * ~
     * @param groupName
     * @return
     */
    public static boolean isConsumerGroupActive(String groupName) {
    	ZkUtils zkUtils = getZkUtils();
        boolean exists = AdminUtils.isConsumerGroupActive(zkUtils,groupName);
        zkUtils.close();
        return exists;
    }

    /**
     * 获取所有消费者组
     * @return
     */
    public static List<String> getConsumerGroups() {
    	ZkUtils zkUtils = getZkUtils();
        List<String> set = JavaConversions.seqAsJavaList(zkUtils.getConsumerGroups()) ;
        zkUtils.close();
        return set;
    }
    /**
     * 根据消费者的名称获取topic
     * @param groupName
     * @return
     */
    public static List<String> getTopicsByConsumerGroup(String groupName) {
    	ZkUtils zkUtils = getZkUtils();
        List<String> set2 = JavaConversions.seqAsJavaList(zkUtils.getTopicsByConsumerGroup(groupName)) ;
        zkUtils.close();
        return set2;
    }
    
    /**
     * 获取排序的BrokerList
     * @return
     */
    public static List<Object>   getSortedBrokerList() {
    	ZkUtils zkUtils = getZkUtils();
        List<Object> set2 = JavaConversions.seqAsJavaList(zkUtils.getSortedBrokerList()) ;
        zkUtils.close();
        return set2;
    }
    public static List<Broker> getAllBrokersInCluster() {
    	ZkUtils zkUtils = getZkUtils();
        List<Broker> set2 = JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster()) ;
        zkUtils.close();
        return set2;
    }
    
    /**
     * 获取消费某个topic发送消息的消费组
     * @param topicName
     * @return
     */
    public static Set<String> getAllConsumerGroupsForTopic(String topicName) {
    	ZkUtils zkUtils = getZkUtils();
        Set<String> stringSeq =  JavaConversions.asJavaSet(zkUtils.getAllConsumerGroupsForTopic(topicName));
        zkUtils.close();
        return stringSeq;
    }

	    /**
	     *  删除topic的某个分区
	     * @param brokerId
	     * @param topicName
	     */
	  public static void deletePartition(int brokerId,String topicName){
		  ZkUtils zkUtils = getZkUtils();
	      zkUtils.deletePartition(brokerId,topicName);
	      zkUtils.close();
	    }

	  /**
	     * 获取所有topic的配置信息
	     */
	    public static Map<String, Properties> fetchAllTopicConfigs(){
	    	ZkUtils zkUtils = getZkUtils();
	    	Map<String, Properties> map = JavaConversions.asJavaMap(AdminUtils.fetchAllTopicConfigs(zkUtils));
	    	zkUtils.close();
	        return map;
	    }

	    /**
	     * 获取指定topic的配置信息
	     * @param topicName
	     */
	    public static Properties fetchEntityConfig(String topicName){
	    	ZkUtils zkUtils = getZkUtils();
	    	Properties prop = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topicName);
	    	zkUtils.close();
	        return prop;
	    }

	    
	    
	    public static Set getAllPartitions(){
	    	ZkUtils zkUtils = getZkUtils();
	    	Set<TopicAndPartition> path = JavaConversions.asJavaSet(zkUtils.getAllPartitions());
	    	zkUtils.close();
	        return path;
	    }

	    public static Map getPartitionsForTopics(String topicName){
	    	ZkUtils zkUtils = getZkUtils();
	    	List<String> list = new ArrayList<String>();
	    	list.add(topicName);
	    	Seq<String> seq = JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
	    	java.util.Map<String, Seq<Object>> map = JavaConverters.asJavaMapConverter(zkUtils.getPartitionsForTopics(seq)).asJava();
	    	zkUtils.close();
	        return map;
	    }
    public static ZkUtils getZkUtils(){
    	Properties props = KafkaConfig.getConsumerConfig();
		ZkUtils zkUtils = ZkUtils.apply(props.getProperty("zookeeper.connect"), 30000,30000,JaasUtils.isZkSecurityEnabled());
		return zkUtils;
    }
    
}
