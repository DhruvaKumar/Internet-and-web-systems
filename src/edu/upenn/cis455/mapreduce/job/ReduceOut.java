package edu.upenn.cis455.mapreduce.job;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import edu.upenn.cis455.mapreduce.Context;

/* @author Dhruva Kumar */

public class ReduceOut implements Context {
	
	File resultFile;

	public ReduceOut(File resultFile) {
		super();
		this.resultFile = resultFile;
	}

	@Override
	public synchronized void write(String key, String value) {
		
		// append reduced key-value pair to resultant file
		FileWriter fw;
		try {
			fw = new FileWriter(this.resultFile, true);
			fw.write(key+"\t"+value+"\r\n");
//			System.out.println(key+"\t"+value);
			fw.flush();
			fw.close();
			
		} catch (IOException e) {
			System.err.println("Error writing into output file!");
			e.printStackTrace();
		}		
		
	}

}
