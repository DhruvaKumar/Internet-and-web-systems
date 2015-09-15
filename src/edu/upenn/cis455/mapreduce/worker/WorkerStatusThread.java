package edu.upenn.cis455.mapreduce.worker;

import java.net.MalformedURLException;
import java.net.URL;

import edu.upenn.cis455.mapreduce.utilities.HTTPClient;
import edu.upenn.cis455.mapreduce.utilities.WorkerStatusDS;

/* @author Dhruva Kumar */

class WorkerStatusThread implements Runnable {

	String masterHost;
	int masterPort;
	WorkerStatusDS wsDS;
	WorkerServlet ws;
	int interval; // seconds
	
	public WorkerStatusThread(WorkerServlet ws, String masterHost, int masterPort, int interval) {
		this.ws = ws;
		this.masterHost = masterHost;
		this.masterPort = masterPort;
		this.interval = interval;
	}
	
	@Override
	public void run() {

		while (true) {
			
			// procure worker status from WorkerServlet
//			this.wsDS = WorkerServlet.wsDS;
			this.wsDS = this.ws.getWorkerStatus();
			
			
			// sends info to Master via HTTP client
			try {
				URL url = new URL("http://"+this.masterHost+":"+this.masterPort+"/workerstatus");
				HTTPClient client = new HTTPClient(url);

				// set the query params: port | status | job | keysRead | keysWritten
				client.setParameter("port", String.valueOf(this.wsDS.getPortListen()));
				client.setParameter("status", this.wsDS.getStatus().toString());
				client.setParameter("job", this.wsDS.getJob());
				client.setParameter("keysRead", String.valueOf(this.wsDS.getKeysRead()));
				client.setParameter("keysWritten", String.valueOf(this.wsDS.getKeysWritten()));

				// send request
				client.sendRequest("GET", false);

				System.out.println("WorkerStatusThread: Worker status sent to Master");
				
			} catch (MalformedURLException e) {
				System.err.println("Invalid URL!");
				e.printStackTrace();
			} catch (Exception e) {
				System.err.println("Error in sending request to Master!");
				e.printStackTrace();
			} finally {
				// wait for an interval 10 seconds
				try { Thread.sleep(this.interval * 1000); } 
				catch (InterruptedException e) { System.out.println("WorkerStatusThread interrupted!"); }
			}
		}// end while true

	}
	
	
	//TODO: close() ?

}
