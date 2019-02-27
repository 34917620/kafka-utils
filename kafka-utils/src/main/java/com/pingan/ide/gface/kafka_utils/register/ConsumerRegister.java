package com.pingan.ide.gface.kafka_utils.register;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.pingan.ide.gface.kafka_utils.concurrent.ConsumerRun;
import com.pingan.ide.gface.kafka_utils.listener.ConsumerListener;

public class ConsumerRegister {
	
	private static final int DEFAULT_MESSAGE_THRESHOLD = 100;
	private static final int DEFAULT_TIMES_THRESHOLD=30000;
	private static final Integer DEFAULT_STREAMS = 1;

	/**已经注册的listener**/
	private static final Map<ConsumerListener,ConsumerRun> REGISTERED_MAP = new ConcurrentHashMap<ConsumerListener, ConsumerRun>();
	
	public static void register(ConsumerListener listener,String topic,String groupId) throws Exception{
    	List<String> topics = Arrays.asList(topic);
    	register(listener,topics,groupId,DEFAULT_STREAMS);
	}
	
	public static void register(ConsumerListener listener,String topic,String groupId,int messageThreshold,int timesThreshold) throws Exception{
    	List<String> topics = Arrays.asList(topic);
    	register(listener,topics,groupId,messageThreshold,timesThreshold,DEFAULT_STREAMS);
	}
	
	public static void register(ConsumerListener listener,String topic,String groupId,Integer streams) throws Exception{
    	List<String> topics = Arrays.asList(topic);
    	register(listener,topics,groupId,streams);
	}
     public static void register(ConsumerListener listener,List<String> topics,String groupId,Integer streams) throws Exception{
    	register(listener,topics,groupId,DEFAULT_MESSAGE_THRESHOLD,DEFAULT_TIMES_THRESHOLD,streams);
	}
     public static void register(ConsumerListener listener,List<String> topics,String groupId) throws Exception{
    	register(listener,topics,groupId,DEFAULT_MESSAGE_THRESHOLD,DEFAULT_TIMES_THRESHOLD,DEFAULT_STREAMS);
	}
     public static void register(ConsumerListener listener,String topic,String groupId,int messageThreshold,int timesThreshold,Integer streams) throws Exception{
    	 List<String> topics = Arrays.asList(topic);
    	 register(listener, topics, groupId, messageThreshold, timesThreshold, streams);
     }
     
     public static void register(ConsumerListener listener,List<String> topics,String groupId,int messageThreshold,int timesThreshold,Integer streams) throws Exception{
    	 //检查listener是否已经注册过
    	 if(REGISTERED_MAP.containsKey(listener)){
    		 throw new Exception("监听已经被注册，不要重复注册");
    	 }
    	 
    	 ConsumerRun cr = new ConsumerRun(topics, groupId, messageThreshold, timesThreshold, listener,streams);
    	 Thread t = new Thread(cr);
    	 t.start();
    	 REGISTERED_MAP.put(listener, cr);
 	}
     
     public static void unregister(ConsumerListener listener){
    	 ConsumerRun cr = REGISTERED_MAP.get(listener);
    	 if(cr!=null){
    		 cr.stop();
    	 }
     }
}
