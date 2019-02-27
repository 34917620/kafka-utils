package com.pingan.ide.gface.kafka_utils.a.demo;

import java.util.List;

import com.pingan.ide.gface.kafka_utils.listener.ConsumerListener;
import com.pingan.ide.gface.kafka_utils.receiver.Receiver;
import com.pingan.ide.gface.kafka_utils.register.ConsumerRegister;
import com.pingan.ide.gface.kafka_utils.sender.Sender;
import com.pingan.ide.gface.kafka_utils.vo.Msg;

public class CommonDemo {
	
	
	
	public static void main(String[] args) {
		//bingListener();
		//receive();
		send();
	}

	//绑定一个监听, 在指定时间里或接到指定消息数据时，触发监听器的方法(最省资源，最高效)
	public static void bingListener(){
		MyListener t1 = new MyListener();
    	String topic = "my-replicated-topic";
    	String groupId = "test1";
    	try {
			ConsumerRegister.register(t1,topic, groupId, 10, 2000); //2000毫秒，10条消息为阀值
		} catch (Exception e) {
			e.printStackTrace();
		}   
	}
	
	//接收数据，循环接收数据，阻塞指定时间，单线程，效率低
	public static void receive(){
		while(true){
			List<Msg> list = Receiver.receive("my-replicated-topic", "test1", 10, 20000);//2000毫秒，10条消息为阀值
			if(list.size()>0){
				System.out.println("读到以下数据："+list.size());
				for(Msg msg:list){
					System.out.println(msg);
				}
			}
		}
	}
	
	//发关数据，循环发送数据到指定topic
	public static void send(){
		for(int i=0;i<100;i++){
		    Sender.send("my-replicated-topic","---"+i);
		}
	}
	
	
	static class MyListener implements ConsumerListener{

		public void execute(List<Msg> messages) {
		    System.out.println("本次接收到的消息如下：");
	         for(Msg s:messages){
	        	 System.out.println(Thread.currentThread().getName()+"---"+s);
	         }
		}
		
	}
}
