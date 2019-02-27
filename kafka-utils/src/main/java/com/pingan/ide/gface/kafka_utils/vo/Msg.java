package com.pingan.ide.gface.kafka_utils.vo;

public class Msg {
   private String topic;
   private String message;
   private long timestamp;
   private int partition;
   private long offset;
   
   public Msg(String topic,String message,long timestamp,int partition,long offset){
	   this.topic = topic;
	   this.message = message;
	   this.timestamp = timestamp;
	   this.partition = partition;
	   this.offset = offset;
   }
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public int getPartition() {
		return partition;
	}
	public void setPartition(int partition) {
		this.partition = partition;
	}
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}
	@Override
	public String toString() {
		return "Msg [topic=" + topic + ", message=" + message + ", timestamp=" + timestamp + ", partition=" + partition
				+ ", offset=" + offset + "]";
	}
   
   
}
