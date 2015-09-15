package edu.upenn.cis455.mapreduce.worker;


import java.io.File;
import java.io.IOException;
import java.lang.reflect.*;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.job.ReduceOut;
import edu.upenn.cis455.mapreduce.utilities.MyTupleMap;
import edu.upenn.cis455.mapreduce.utilities.SyncReaderReduce;

/* @author Dhruva Kumar */

public class WorkerReduceThread implements Runnable {
	SyncReaderReduce sReader;
	File resultFile;
	Object job;
	WorkerServlet ws;

	public WorkerReduceThread(WorkerServlet ws, SyncReaderReduce sReader, File resultFile, Object job) {
		super();
		this.sReader = sReader;
		this.resultFile = resultFile;
		this.job = job;
		this.ws = ws;
	}

	@Override
	public void run() {
		
//		System.out.println("reached outside");
		// while all the files are read
		while (!sReader.doneReading()) {
			try {
				// read the tuple(key-value pair) from the file(s)
				MyTupleMap tuple = sReader.read();
//				System.out.println("reacehd here in run?"+tuple);
				// reduce
				if (tuple != null) {
					
					// update keysRead
					int keys = tuple.getValue().length;
					this.ws.incrementKeysRead(keys);
					
					// invoke 'reduce'
					try {
						Method methodMap = this.job.getClass().getMethod("reduce", String.class, String[].class, Context.class);
						Context context = new ReduceOut(this.resultFile);
						methodMap.invoke(this.job, tuple.getKey(), tuple.getValue(), context);
						
						// update keysWritten
						this.ws.incrementKeysWritten(1);
						
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
