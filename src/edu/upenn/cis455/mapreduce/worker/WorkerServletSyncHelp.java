package edu.upenn.cis455.mapreduce.worker;

import java.io.File;

/* @author Dhruva Kumar */

public class WorkerServletSyncHelp {

	WorkerServlet ws;

	public WorkerServletSyncHelp (WorkerServlet ws) {
		this.ws = ws;
	}

	public synchronized void incrementPushCounter() {
		this.ws.pushDataCounter++;
		if (this.ws.pushDataCounter == this.ws.numWorkers) {
			this.ws.pushDataReceived = true;
			synchronized (this.ws.Lock) {
				this.ws.Lock.notifyAll();
			}
		}
	}
	public synchronized File createSpoolInFile(){

		// create spool-in dir
		File spoolInDir = new File(this.ws.storageDir, "spool-in");
		// for new /runmap
		if (this.ws.spoolInIndex == 1) {
			// if exists from before, delete contents
			if (spoolInDir.exists()) {
				// delete 
				String [] files = spoolInDir.list();
				for (String s : files) {
					File file = new File(spoolInDir.getPath(), s);
					file.delete();
				}
			}
			// create a new spool-in directory
			new File(this.ws.storageDir, "spool-in").mkdir();
		}

		// create new file and return 
		File f = new File(this.ws.storageDir, "spool-in/spool"+(this.ws.spoolInIndex)+".txt");
		this.ws.spoolInIndex++;

		//		// testing
		//		System.out.println(Thread.currentThread().getName()+" is spooling in...");
		//		Thread.sleep(10000);

		return f;
	}

}
