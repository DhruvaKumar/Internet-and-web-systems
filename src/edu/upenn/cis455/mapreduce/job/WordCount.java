package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

/* @author Dhruva Kumar */

public class WordCount implements Job {

	public void map(String key, String value, Context context)
	{
		// key: split value on whitespace | value: 1 -> spool it out
		String [] line = value.split("\\s+");
		for (int i = 0; i<line.length; i++) {
			context.write(line[i], "1");
		}

	}

	public void reduce(String key, String[] values, Context context)
	{
		// sum ( (int) values)
		int [] valuesInt = new int[values.length];
		int value = 0;
		for (int i = 0; i<values.length; i++) {
			try { valuesInt[i] = Integer.parseInt(values[i]); }
			catch (Exception e) { 
				System.err.println("reduce: Couldn't parse string into integer"+values[i]); 
				valuesInt[i] = 0; 
			}
			value += valuesInt[i];
		}
	
		context.write(key, Integer.toString(value));
		
//		System.out.println("Reduce: "+key+": "+value);
		
		

	}

}
