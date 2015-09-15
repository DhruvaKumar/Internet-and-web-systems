package edu.upenn.cis455.mapreduce.utilities;

/* @author Dhruva Kumar */

public class MyTupleMap {
	String key; 
	String [] value;
	
	public MyTupleMap(String key, String[] value) {
		super();
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String[] getValue() {
		return value;
	}

	public void setValue(String[] value) {
		this.value = value;
	}
	
	
	
	

	
	
}
