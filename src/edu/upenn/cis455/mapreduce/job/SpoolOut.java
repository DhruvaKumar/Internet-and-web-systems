package edu.upenn.cis455.mapreduce.job;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;

import edu.upenn.cis455.mapreduce.Context;
import org.apache.commons.codec.digest.DigestUtils;

/* @author Dhruva Kumar */

public class SpoolOut implements Context{

	String storageDir;
	int numWorkers;
	static BigInteger total = new BigInteger("ffffffffffffffffffffffffffffffffffffffff", 16);
	BigInteger split;
	BigInteger totalBySplit;
	
	
	public SpoolOut(String storageDir, int numWorkers) {
		this.storageDir = storageDir;
		this.numWorkers = numWorkers;
		this.split = new BigInteger(String.valueOf(numWorkers));
		this.totalBySplit = SpoolOut.total.divide(split);
	}
	
	// TODO: added synchronized
	@Override
	public synchronized void write(String key, String value) {
		
		// Hash the key
		String digest = DigestUtils.sha1Hex(key);
		
		// Write it into corresponding file in the spool out directory
		int bucket = new BigInteger(digest,16).divide(totalBySplit).intValue();
		File f = new File(this.storageDir, "spool-out/spool"+(bucket+1)+".txt");
		// pre-cautionary measure
		try {
			//if (!f.exists()) { f.createNewFile(); }
			FileWriter fw = new FileWriter(f, true);
			fw.write(key+"\t"+value+" \r\n");
			fw.close();
			
		} catch (IOException e) {
			System.err.println("Error while spooling out!");
			e.printStackTrace();
		}
		
		//System.out.println("Spooled out!");
//		System.out.println("SpoolOut: "+Thread.currentThread().getName()+"|"+key + "|"+digest+"|"+bucket);
		
//		/// testing- log into file
//		File fTest = new File(this.storageDir, "log.txt");
//		FileWriter fww;
//		try {
//			fww = new FileWriter(fTest, true);
//			fww.write("SpoolOut: "+Thread.currentThread().getName()+"|"+key + "|"+digest+"|"+bucket+"\r\n");
//			fww.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		
		
	}

}
