package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.utilities.HTTPClient;
import edu.upenn.cis455.mapreduce.utilities.Status;
import edu.upenn.cis455.mapreduce.utilities.WorkerStatusDS;

/* @author Dhruva Kumar */

public class MasterServlet extends HttpServlet {

	static final long serialVersionUID = 455555001;
	boolean runMapSent = false;
	boolean runReduceSent = false;
	String job;
	boolean isJobFile = false;
	String outputDir;
	int numThreadsReduce;
	ArrayList<String> activeWorkers; // keySet(): hostPort
//	String storageDir;
//	String jobFile;
	
	// stores a hashmap of the status of all the workers. 
	// key = ip:port | value = WorkerStatusDS (status, job, keysRead, keysWritten, lastReceived)
	HashMap<String, WorkerStatusDS> statusWorkers = new HashMap<String, WorkerStatusDS>();

	
//	public void init(ServletConfig servletConfig) throws ServletException {
//
//		// read init params from web.xml
//		this.storageDir = servletConfig.getInitParameter("storagedir");
//	
//		// create storage dir if not already present (delete previous contents if present)
//		File storMast = new File (this.storageDir);
//		if (storMast.exists()) {
//			String [] files = storMast.list();
//			for (String s : files) {
//				File file = new File(storMast.getPath(), s);
//				file.delete();
//			}
//		}
//		// create the directory
//		new File(this.storageDir).mkdir();
//	}

	/*============================================================= doPost() ============================================================ */
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		//	  out.println("Receieved form!");

		// ONLY IF RUNMAP OR REUNREDUCE IS NOT SENT, otherwise ignore
		if (!runMapSent && !runReduceSent) {
	
			/* ------------------------------------------------------get parameters from form ----------------------------------------------*/
			
			this.job = request.getParameter("job");
			String inputDir = request.getParameter("inputDir");
			this.outputDir = request.getParameter("outputDir");
			String numThreadsMapStr = request.getParameter("numThreadsMap");
			int numThreadsMap = 5;
			try { numThreadsMap = Integer.parseInt(numThreadsMapStr); }
			catch (Exception e) { System.err.println("Could not parse numThreadsMap. Using the default value: 5"); }
			String numThreadsReduceStr = request.getParameter("numThreadsReduce");
			this.numThreadsReduce = 5;
			try { this.numThreadsReduce = Integer.parseInt(numThreadsReduceStr); }
			catch (Exception e) { System.err.println("Could not parse numThreadsReduce. Using the default value: 5"); }
			
//			String inputDir = "";
//			String numThreadsMapStr = "";
//			String numThreadsReduceStr = "";
//			int numThreadsMap = 5;
//			numThreadsReduce = 5;
//			InputStream fileContent = null;
//			String jobFileName = "sample.Class";
//			OutputStream os = null;
//			// extra credit: dynamic deployment: load class and write it into local disk
//			try {
//				List<FileItem> items = new ServletFileUpload(new DiskFileItemFactory()).parseRequest(request);
//				for (FileItem item : items) {
//					// process normal form
//					if (item.isFormField()) {
//						
//						if (item.getFieldName().equals("job")) { this.job = item.getString(); } 
//						else if (item.getFieldName().equals("inputDir")) { inputDir = item.getString(); }
//						else if (item.getFieldName().equals("outputDir")) { outputDir = item.getString(); }
//						else if (item.getFieldName().equals("numThreadsMap")) { 
//							numThreadsMapStr = item.getString(); 
//							try { numThreadsMap = Integer.parseInt(numThreadsMapStr); }
//							catch (Exception e) { System.err.println("Could not parse numThreadsMap. Using the default value: 5"); }
//						}
//						else if (item.getFieldName().equals("numThreadsReduce")) { 
//							numThreadsReduceStr = item.getString(); 
//							try { numThreadsReduce = Integer.parseInt(numThreadsReduceStr); }
//							catch (Exception e) { System.err.println("Could not parse numThreadsReduce. Using the default value: 5"); }
//						}
//						
//					} 
//					// process job file
//					else if (!("").equals(FilenameUtils.getName(item.getName()))){
//						
//						this.isJobFile = true;
//						// Process file (input type="file"
//						jobFileName = FilenameUtils.getName(item.getName());
//						fileContent = item.getInputStream();
//						
//						jobFile = convertStreamToString(fileContent);
//						System.out.println(jobFile);
//						
//						// write the input stream to file (for debugging)
//						fileContent = item.getInputStream();
//						os = new FileOutputStream(new File(this.storageDir, jobFileName));
//						int read = 0;
//						byte[] bytes = new byte[1024];
//						while ((read=fileContent.read(bytes)) != -1) {
//							os.write(bytes, 0, read);
//						}
//						
//						
//						System.out.println("Saved the job class file on the local disk!");						
//					}
//				}
//
//			} catch (FileUploadException e1) {
//				System.err.println("Error uploading file!");
//				e1.printStackTrace();
//				out.println("Error uploading file!");
//				displayStatusPage(request, response, out);
//				return;
//			} finally {
//				if (os!=null) os.close();
//				if (fileContent!=null) fileContent.close();
//			}
			
//			System.out.println("job: "+job);
//			System.out.println("inputDir: "+inputDir);
//			System.out.println("outputDir: "+outputDir);
//			System.out.println("numThreadsMap: "+numThreadsMap);
//			System.out.println("numThreadsReduce: "+numThreadsReduce);
//			System.out.println("isJobFile: "+this.isJobFile);
			
			// handle empty params
//			if (((job == null || ("").equals(job)) && !this.isJobFile) || inputDir.isEmpty() || outputDir.isEmpty() || numThreadsMapStr.isEmpty() || numThreadsReduceStr.isEmpty()) {
			if (job.isEmpty() || inputDir.isEmpty() || outputDir.isEmpty() || numThreadsMapStr.isEmpty() || numThreadsReduceStr.isEmpty()) {
				out.println("Please enter all the fields!");
				displayStatusPage(request, response, out);
				return;
			} 
			
//			// the string has more precedence over the class file if both are given
//			if (("").equals(job)) {
//				job = jobFileName;
//				System.out.println("Sending job file...");
//			} else {
//				isJobFile = false;
//				System.out.println("Sending job string...");
//			}
	
			/* ---------------------------------------------  send /runmap to all active workers -------------------------------------------------*/
	
			System.out.println("Master(1): Sending /runmap to all active workers...");
			this.activeWorkers = getActiveWorkers(); 
	
			for (String hostPort : this.activeWorkers) {
					try {
//						System.out.println("Sent to : http://"+ hostPort +"/runmap");
						URL url = new URL("http://"+ hostPort +"/runmap");
						HTTPClient client = new HTTPClient(url);
	
						// set the query parameters: job | input | numThreads | numWorkers | worker1, worker2, ... workern
						
						// extra credit: send another parameter: isJobFile
//						if (isJobFile) {
//							client.setParameter("jobFile", jobFile);
//						}
						
						client.setParameter("job", job);
						client.setParameter("input", inputDir);
						client.setParameter("numThreads", String.valueOf(numThreadsMap));
						client.setParameter("numWorkers", String.valueOf(activeWorkers.size()));
						for (int i = 0; i<activeWorkers.size(); i++) {
							client.setParameter("worker"+(i+1), activeWorkers.get(i));
						}
						// add header
						client.setHeader("Content-Type", "application/x-www-form-urlencoded");
	
						// send request
						client.sendRequest("POST", false);
	
//						System.out.println("Master: /runmap sent to "+hostPort);
	
					} catch (MalformedURLException e) {
						System.err.println("Invalid URL!");
						e.printStackTrace();
					} catch (Exception e) {
						System.err.println("Error in sending request to Master!");
						e.printStackTrace();
					}			  
			} // end for /runmap
			
			int numWorkers = 0;
			if (activeWorkers != null) { numWorkers = activeWorkers.size(); } 
			System.out.println("Master(2): Sent /runmap to all active workers!");
			System.out.println("Master(2a): Number of active workers: "+numWorkers);
			System.out.println("Master(3): Waiting to send /runreduce...");
	
			// set runMapSent true
			this.runMapSent = true;
			
		} // if !runMapSent
		else {
			out.println("Previous map-reduce job is being processed! Please wait until that gets over!");
		}


		out.close();
	}

	private ArrayList<String> getActiveWorkers() {

		ArrayList<String> hostPortArray = new ArrayList<String>();
		for (String hostPort : statusWorkers.keySet()) {
			WorkerStatusDS ws = statusWorkers.get(hostPort);
			// if active: received in the last 30 seconds
			if ((new Date().getTime()) - ws.getLastReceived().getTime() < 30000) {
				hostPortArray.add(hostPort);
			}
		}
		return hostPortArray;
	}

//	static String convertStreamToString(InputStream is) {
//	    Scanner s = new Scanner(is).useDelimiter("\\A");
//	    return s.hasNext() ? s.next() : "";
//	}

	/*============================================================= doGet() ============================================================ */

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		response.setContentType("text/html");
		PrintWriter out = response.getWriter();

		/* ------------------------------------------------ Status updates: /workerstatus -----------------------------------------------*/

		// Listen to /workerstatus from the WorkerServlet(s) and store the information
		if (request.getServletPath().equals("/workerstatus")) {

			out.println("<html><head><title>Master</title></head>");
			out.println("<body>");

			// read the parameters
			String port = request.getParameter("port");
			String statusStr = request.getParameter("status");
			Status status = getStatus(statusStr);
			if (port != null & status != null) {
				String remoteHost = request.getRemoteHost();
				String job = request.getParameter("job");
				int keysRead = 0;
				int keysWritten = 0;
				if (request.getParameter("keysRead") != null) {
					keysRead = Integer.parseInt(request.getParameter("keysRead"));
				}
				if (request.getParameter("keysWritten") != null) {
					keysWritten = Integer.parseInt(request.getParameter("keysWritten"));
				}
				
				// store it
				WorkerStatusDS wsDS = new WorkerStatusDS(status, job, keysRead, keysWritten, new Date());
				statusWorkers.put(remoteHost+":"+port, wsDS);
				
				
				
				// /runreduce: if runMapSent & all active workers are waiting
				if (this.activeWorkers != null) {
					boolean activeWorkersWaiting = true;
					for (String aw : this.activeWorkers) {
						if (this.statusWorkers.containsKey(aw) && this.statusWorkers.get(aw).getStatus() != Status.waiting) {
							activeWorkersWaiting = false; 
							break;
						}
					}
					if (this.runMapSent && activeWorkersWaiting) {
						System.out.println("Master(4): /runmap for all workers completed! Sending /runreduce...");
						sendRunReduce();
						System.out.println("Master(5): /runreduce sent to all active workers!");
						this.runReduceSent = true;
					}
					
					
					
					// reset: if runMapSent & runReduceSent & all active workers are idle
					boolean activeWorkersIdle = true;
					for (String aw : this.activeWorkers) {
						if (this.statusWorkers.containsKey(aw) && this.statusWorkers.get(aw).getStatus() != Status.idle) {
							activeWorkersIdle = false; 
							break;
						}
					}
					if (this.runMapSent && this.runReduceSent && activeWorkersIdle) {
						System.out.println("Master(6): /runreduce completed for all active workers! Resetting!...");
						this.runMapSent = false;
						this.runReduceSent = false;
					}
				}
				
				// received
				out.println("Received worker status!");
			}
			out.println("<hr>");
			out.println("</body></html>");
		}


		/* ------------------------------------------------ Status page: /status -----------------------------------------------*/
		
		// 1. display web form for submitting jobs (map/reduce)
		// 2. display the status of all the workers
		
		if (request.getServletPath().equals("/status")) {

			displayStatusPage(request, response, out);  	
		}


		/* ----------------------------------------------------- debugging ------------------------------------------------------------*/
		System.out.println("------------------------------------------------------------");
		//    System.out.println("getConextPath(): "+request.getContextPath());
		System.out.println("getServletPath(): "+request.getServletPath());
		System.out.println("getServerName(): "+request.getServerName());
		// ip of worker
		System.out.println("getRemoteHost(): "+request.getRemoteHost());
		System.out.println("getServerPort(): "+request.getServerPort());
		// params
		System.out.println("getParamter(\"port\"): "+request.getParameter("port"));
		System.out.println("getParamter(\"status\"): "+request.getParameter("status"));

		out.close();
	}
	
	/*======================================================= sendRunReduce() ============================================================ */
	
	private void sendRunReduce() {

		for (String hostPort : this.activeWorkers) {
			try {
				
				URL url = new URL("http://"+ hostPort +"/runreduce");
				HTTPClient client = new HTTPClient(url);

				// set the query parameters: job | output | numThreads
				
				// extra credit: send another parameter: isJobFile
//				if (isJobFile) {
//					client.setParameter("jobFile", jobFile);
//				}
				
				client.setParameter("job", job);
				client.setParameter("output", outputDir);
				client.setParameter("numThreads", String.valueOf(numThreadsReduce));
				// add header
				client.setHeader("Content-Type", "application/x-www-form-urlencoded");

				// send request
				client.sendRequest("POST", false);

			} catch (MalformedURLException e) {
				System.err.println("Invalid URL!");
				e.printStackTrace();
			} catch (Exception e) {
				System.err.println("Error in sending request to Master!");
				e.printStackTrace();
			}			  
		} // end for /runreduce
		
	}// sendRunReduce
	
	
	/*======================================================= displayStatusPage() ============================================================ */
	
	private void displayStatusPage(HttpServletRequest request, HttpServletResponse response, PrintWriter out) throws ServletException, IOException {

		// 1. display web form for submitting jobs (map/reduce)
		// 2. display the status of all the workers
		
		// 1. display web form to submit jobs
		request.getRequestDispatcher("status.html").include(request, response);


		// 2. display status of "active" workers (active: who have updated within the last 30 seconds)
		out.println("<h4 align=\"center\">Worker Status:</h4> </br>");
		out.println("<table align=\"center\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\" style=\"width: 500px;\">");
		out.println("<thead><tr>");
		out.println("<th scope=\"col\">ip:port</th>");
		out.println("<th scope=\"col\">status</th>");
		out.println("<th scope=\"col\">job</th>");
		out.println("<th scope=\"col\">keysRead</th>");
		out.println("<th scope=\"col\">keysWritten</th>");
		out.println("</tr></thead>");
		out.println("<tbody>");
		for (String key : statusWorkers.keySet()) {
			WorkerStatusDS ws = statusWorkers.get(key);
			// if active: received in the last 30 seconds
			if ((new Date().getTime()) - ws.getLastReceived().getTime() < 30000) {
				out.println("<tr>");
				out.println("<td style=\"text-align: center; vertical-align: middle;\">"+key+"</td>");
				out.println("<td style=\"text-align: center; vertical-align: middle;\">"+ws.getStatus()+"</td>");
				out.println("<td style=\"text-align: center; vertical-align: middle;\">"+ws.getJob()+"</td>");
				out.println("<td style=\"text-align: center; vertical-align: middle;\">"+ws.getKeysRead()+"</td>");
				out.println("<td style=\"text-align: center; vertical-align: middle;\">"+ws.getKeysWritten()+"</td>");
				out.println("</tr>");
			}
		}
		out.println("</tbody>");
		out.println("</table>");   
	}

	/*============================================================= getStatus() ============================================================ */

	// given the status string, returns the corresponding enum type. if nothing matches, returns null
	Status getStatus (String status) {
		if ("mapping".equalsIgnoreCase(status)) {
			return Status.mapping;
		}
		if ("waiting".equalsIgnoreCase(status)) {
			return Status.waiting;
		}
		if ("reducing".equalsIgnoreCase(status)) {
			return Status.reducing;
		}
		if ("idle".equalsIgnoreCase(status)) {
			return Status.idle;
		}
		return null;  
	}
}

