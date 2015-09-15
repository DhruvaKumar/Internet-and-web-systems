package edu.upenn.cis455.mapreduce.worker;


import java.io.IOException;
import java.lang.reflect.*;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.job.SpoolOut;
import edu.upenn.cis455.mapreduce.utilities.MyTuple;
import edu.upenn.cis455.mapreduce.utilities.SyncReader;

/* @author Dhruva Kumar */

public class WorkerMapThread implements Runnable {
	SyncReader sReader;
	String absInputDir; // storage + relative i/p dir  path
	String storageDir;
	Object job;
	int numWorkers;
	WorkerServlet ws;

	public WorkerMapThread(WorkerServlet ws, SyncReader sReader, String storageDir, String inputDir, Object job, int numWorkers) {
		super();
		this.sReader = sReader;
		this.storageDir = storageDir;
		this.absInputDir = storageDir+inputDir;
		this.job = job;
		this.numWorkers = numWorkers; 
		this.ws = ws;
	}

	@Override
	public void run() {
		
		// while all the files are read
		while (!sReader.doneReading()) {
			try {
				// read the tuple(key-value pair) from the file(s)
				MyTuple tuple = sReader.read();
				
				// map
				if (tuple != null) {
					// update keysRead
					this.ws.incrementKeysRead(1);
					
					// invoke 'map'
					try {
						Method methodMap = this.job.getClass().getMethod("map", String.class, String.class, Context.class);
						Context context = new SpoolOut(this.storageDir, this.numWorkers);
						methodMap.invoke(this.job, tuple.getKey(), tuple.getValue(), context);
						
						// update keysWritten
						int keys = tuple.getValue().split("\\s+").length;
						this.ws.incrementKeysWritten(keys);
						
					} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						e.printStackTrace();
					}
				}
				
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		} // while !doneReading
	
	} // run()
	
	
}
