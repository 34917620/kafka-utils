package com.pingan.ide.gface.kafka_utils.concurrent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.pingan.ide.gface.kafka_utils.factory.KafkaFactroy;
import com.pingan.ide.gface.kafka_utils.listener.ConsumerListener;
import com.pingan.ide.gface.kafka_utils.utils.StopWatch;
import com.pingan.ide.gface.kafka_utils.vo.Msg;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class ConsumerRun implements Runnable {

	private int messageThreshold;  //消息条数阀值
	private int timesThreshold;    //时间阀值（s)
	private ConsumerListener listener; //监听器
	private List<String> topics;
	private String groupId;
	private Integer steams; //流的数量，这里默认取1个
	
	private volatile boolean isStop = false;
	
	public ConsumerRun(List<String> topics,String groupId,int messageThreshold,int timesThresHold,ConsumerListener listener,Integer streams){
		this.messageThreshold = messageThreshold;
		this.timesThreshold = timesThresHold;
		this.listener = listener;
		this.topics = topics;
		this.groupId = groupId;
		this.steams = streams;
	}
	
	public void run() {
		
		ConsumerConnector cc = KafkaFactroy.getConsumerConnector(groupId);
		Map<String,Integer> topicStreamCountMap = new HashMap<String,Integer>();
		for(String topic:topics){
			topicStreamCountMap.put(topic, steams);
		}
		Map<String,List<KafkaStream<byte[],byte[]>>> messageStreams = cc.createMessageStreams(topicStreamCountMap);
	    final BlockingQueue<Msg> queue = new LinkedBlockingQueue<Msg>(messageThreshold);
		
		//得到所有topic的所有stream,开启子线程乾监听消息流
		for(Iterator<List<KafkaStream<byte[],byte[]>>> it=messageStreams.values().iterator();it.hasNext();){
			List<KafkaStream<byte[],byte[]>> kafkaStreams =  it.next();
			for(KafkaStream<byte[],byte[]> stream:kafkaStreams){
				ReadStreamRun readStreamRun = new ReadStreamRun(queue,stream);
				Thread t = new Thread(readStreamRun);
				t.setDaemon(true);
				t.start();
			}
		}
		StopWatch stopWatch = new StopWatch(timesThreshold);
		while(true){
			if(stopWatch.timeout()||queue.size()==messageThreshold){  //超过设定的阀值，马上调用listener方法
				List<Msg> messages = new LinkedList<Msg>();
				for(int i=0;i<messageThreshold;i++){
					Msg msg = queue.poll();
					if(msg==null){
						break;
					}
					messages.add(msg);
				}
				if(messages.size()>0){
				   listener.execute(messages);
				}
				stopWatch.reset();
			}
			if(isStop){
				//先让每个子线程的流关闭，然后把queue的数据清空处理
				//关闭和Kafka的连接，这样会导致stream.hashNext返回false
				cc.shutdown();
				if(queue.size()>0){
					List<Msg> messages = new LinkedList<Msg>();
					for(int i=0;i<messageThreshold;i++){
						Msg msg = queue.poll();
						if(msg==null){
							break;
						}
						messages.add(msg);
					}
					if(messages.size()>0){
					   listener.execute(messages);
					}
				}
				
				return;
			}
			try {
				Thread.sleep(500);//避免循环过快，消耗cpu资源
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		}
	}
	
	public void stop(){
		isStop = true;
	}

	/**
	 * 
	 * @author 守护线程，负责收集各个stream上的消息，再汇总到queue
	 *
	 */
	static class ReadStreamRun implements Runnable{

		private BlockingQueue<Msg> queue;
		private KafkaStream<byte[],byte[]> stream;
		
		public ReadStreamRun(BlockingQueue<Msg> queue,KafkaStream<byte[],byte[]> stream){
			this.queue = queue;
			this.stream = stream;
		}
		
		public void run() {
			ConsumerIterator<byte[],byte[]> it = stream.iterator();
			while(it.hasNext()){
				MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
		
				Msg msg = new Msg(messageAndMetadata.topic(),new String(messageAndMetadata.message()),messageAndMetadata.timestamp(),messageAndMetadata.partition(),messageAndMetadata.offset());
				try {
					queue.put(msg);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
}
