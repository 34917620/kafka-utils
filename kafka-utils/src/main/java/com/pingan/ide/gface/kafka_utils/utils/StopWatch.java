package com.pingan.ide.gface.kafka_utils.utils;

public class StopWatch {

	private volatile long startTime;
	private int threshold;
	
    public StopWatch(int threshold){
    	this.threshold = threshold;
    	startTime = System.currentTimeMillis();
    }
	
	public void start(){
		startTime = System.currentTimeMillis();
	}
	
	public boolean timeout(){
		boolean flag = (System.currentTimeMillis()-startTime)>threshold;
		if(flag){
			reset();
		}
		return flag;
	}
	
	public void reset(){
		startTime = System.currentTimeMillis();
	}
}
