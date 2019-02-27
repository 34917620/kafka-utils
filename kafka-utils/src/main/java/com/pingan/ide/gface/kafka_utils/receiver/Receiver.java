package com.pingan.ide.gface.kafka_utils.receiver;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.pingan.ide.gface.kafka_utils.factory.KafkaFactroy;
import com.pingan.ide.gface.kafka_utils.utils.StopWatch;
import com.pingan.ide.gface.kafka_utils.vo.Msg;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * 接收类
 * @author wangzhi802
 *
 */
public class Receiver {
      
	
	public static List<Msg> receive(String topic,String groupId,int messagesThreshold,int timesThreshold){
		ConsumerConnector cc = KafkaFactroy.getConsumerConnector(groupId);
		Map<String,Integer> topicStreamCountMap = new HashMap<String,Integer>();
		topicStreamCountMap.put(topic, 1);
		Map<String,List<KafkaStream<byte[],byte[]>>> messageStreams = cc.createMessageStreams(topicStreamCountMap);
	    KafkaStream<byte[],byte[]> stream = messageStreams.get(topic).get(0); //因为只开一个stream,直接取第一条
	    final BlockingQueue<Msg> queue = new LinkedBlockingQueue<Msg>(messagesThreshold);
	    ReadStreamRun readStreamRun = new ReadStreamRun(queue,stream,messagesThreshold);
		Thread t = new Thread(readStreamRun);
		t.setDaemon(true);
		t.start();
		List<Msg> result = new LinkedList<Msg>();
		StopWatch stopWatch = new StopWatch(timesThreshold);
		
		while(true){
			if(stopWatch.timeout()||queue.size()==messagesThreshold){  //超过设定的阀值
				for(int i=0;i<messagesThreshold;i++){
					Msg msg = queue.poll();
					if(msg==null){
						break;
					}
					result.add(msg);
				}
				break;
			}
			try {
				Thread.sleep(500);//避免循环过快，消耗cpu资源
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
		}
		cc.shutdown();
		return result;
	}
	
	/**
	 * 
	 * @author 守护线程，负责收集各个stream上的消息，再汇总到queue
	 *
	 */
	static class ReadStreamRun implements Runnable{

		private BlockingQueue<Msg> queue;
		private KafkaStream<byte[],byte[]> stream;
		private int messagesThreshold;
		
		public ReadStreamRun(BlockingQueue<Msg> queue,KafkaStream<byte[],byte[]> stream,int messagesThreshold){
			this.queue = queue;
			this.stream = stream;
			this.messagesThreshold = messagesThreshold;
		}
		
		public void run() {
			ConsumerIterator<byte[],byte[]> it = stream.iterator();
			while(it.hasNext()){
				MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
				Msg msg = new Msg(messageAndMetadata.topic(),new String(messageAndMetadata.message()),messageAndMetadata.timestamp(),messageAndMetadata.partition(),messageAndMetadata.offset());
				try {
					queue.put(msg);
					if(queue.size()==messagesThreshold){
						break;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
}
