package edu.upenn.cis455.mapreduce.worker;

import java.io.*;

import javax.servlet.*;
import javax.servlet.http.*;


import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.FileChannel;

import edu.upenn.cis455.mapreduce.utilities.HTTPClient;
import edu.upenn.cis455.mapreduce.utilities.Status;
import edu.upenn.cis455.mapreduce.utilities.SyncReader;
import edu.upenn.cis455.mapreduce.utilities.SyncReaderReduce;
import edu.upenn.cis455.mapreduce.utilities.WorkerStatusDS;

/* @author Dhruva Kumar */

public class WorkerServlet extends HttpServlet {

	static final long serialVersionUID = 455555002;
	
	// init params
	String storageDir;
	String masterHost;
	int masterPort;
	int portListen = 3001;
	Thread wsThread;
	
	WorkerStatusDS wsDS; 
	int spoolInIndex;
	int numWorkers;
	int pushDataCounter; 
	boolean pushDataReceived;
	WorkerServletSyncHelp wsSync;
	Object Lock;
	volatile int keysRead;
	volatile int keysWritten;


	public WorkerServlet () {
	}
	
	/*============================================================= init() ============================================================ */
	
	// called initially before any method
	public void init(ServletConfig servletConfig) throws ServletException {
		
		// read init params from web.xml
		this.storageDir = servletConfig.getInitParameter("storagedir");
		try { this.portListen = Integer.parseInt(servletConfig.getInitParameter("port")); }
		catch (Exception e) {
			System.err.println("Could not parse the port number! Please enter a valid numeric port number in web.xml!");
			e.printStackTrace();
			System.exit(1);
		}
		
		try {
			String [] masterHostPort = servletConfig.getInitParameter("master").split(":");
			this.masterHost = masterHostPort[0].trim();
			this.masterPort = Integer.parseInt(masterHostPort[1].trim());
		} catch (Exception e) {
			System.err.println("Error parsing master host and port! Please input correct format in web.xml!");
			e.printStackTrace();
			System.exit(1);
		}
		
		// init WorkerServletSyncHelp to call synchronized methods from this servlets
		this.wsSync = new WorkerServletSyncHelp(this);
		
		// initialize spool-in index
		this.spoolInIndex = 1; // reset
		this.numWorkers = 0;
		this.pushDataCounter = 0; //reset
		this.pushDataReceived = false; // reset 
		this.Lock = new Object();
		this.keysRead = 0; // reset
		this.keysWritten = 0; 
		
		/* ------ spawns threads  ------- */

		// 1. /workerstatus : sends its status every 10 seconds
		System.out.println("Starting worker status threads...");
		this.wsDS = new WorkerStatusDS(this.portListen, Status.idle, null, 0, 0);
		WorkerStatusThread ws = new WorkerStatusThread(this, this.masterHost, this.masterPort, 10);
		this.wsThread = new Thread(ws, "WorkerStatusThread");
		// TODO daemon required
		wsThread.start();
		
		// pid 		
		System.out.println("pid: "+ManagementFactory.getRuntimeMXBean().getName());

	}

	  public WorkerStatusDS getWorkerStatus() {
		  return this.wsDS;
	  }
	  public synchronized void incrementKeysRead (int keys) {
		  this.keysRead += keys;
		  this.wsDS.setKeysRead(this.keysRead);
	  }
	  public synchronized void incrementKeysWritten (int keys) {
		 this.keysWritten += keys;
		 this.wsDS.setKeysWritten(this.keysWritten);
	  }
	  
	  /*============================================================= doPost() ============================================================ */

	public void doPost(HttpServletRequest request, HttpServletResponse response)  throws IOException {
		
		response.setContentType("text/html");

		/* --------------------------------- MAP /runmap -----------------------------------*/

		if (request.getServletPath().equals("/runmap")) {
			
			runMap(request);
		} 

		/* ------------------------------- REDUCE /runreduce -------------------------------*/

		else if (request.getServletPath().equals("/runreduce")) {
			
			runReduce(request);
		}
		
		/* ------------------------------ PUSHDATA /pushdata -------------------------------*/

		else if (request.getServletPath().equals("/pushdata")) {
			
			receivePushData(request);	
		} 

		
	} // end of doPost()
	
	/*============================================================= runMap() ============================================================ */
	
	public void runMap(HttpServletRequest request) throws IOException {
		
		System.out.println("-----------------------------------------------------------");
		System.out.println("Received /runmap! [8]");
		
		

		/* --------------------------------------------------- read in the parameters ------------------------------------------------------*/
		String jobStr = request.getParameter("job");		
		String inputDir = request.getParameter("input");
		String numThreadsStr = request.getParameter("numThreads");
		int numThreads = 0;
		try { numThreads = Integer.parseInt(numThreadsStr); }
		catch (Exception e) {}
		String numWorkersStr = request.getParameter("numWorkers");
//		int numWorkers = 0;
		synchronized (this) {
			try { this.numWorkers = Integer.parseInt(numWorkersStr); }
			catch (Exception e) {}
		}
		String [] workerIPPORT = new String[numWorkers];
		for (int i = 0; i<numWorkers; i++) {
			workerIPPORT[i] = request.getParameter("worker"+(i+1));
			if (workerIPPORT[i] != null) { workerIPPORT[i] = workerIPPORT[i].trim(); }
		}
//		System.out.println(numWorkersStr+"|"+numWorkers);
		
		// extra credit:
//		String jobFile = request.getParameter("jobFile");
//		if (jobFile != null) {
//			System.out.println(jobFile);
//			OutputStream os = new FileOutputStream("classes/"+jobStr);
//		    os.write(jobFile.getBytes());
//		    os.close();
//		    // remove the .class from say WordCount.class
//		    jobStr = jobStr.substring(0,jobStr.lastIndexOf("."));
//		}
		
		// change status
		synchronized (this) {
			this.wsDS.setStatus(Status.mapping);
			this.wsDS.setJob(jobStr);
			this.wsDS.setKeysRead(0);
			this.wsDS.setKeysWritten(0);
			this.keysRead = 0;
			this.keysWritten = 0;
		}


		try {
			/* ---------------------------------- instantiate the job (WordCount) class ---------------------------------------------*/
			Object job = Class.forName(jobStr).newInstance();

			// Instantiate a SyncReader and initialize it
			SyncReader sReader = new SyncReader(this.storageDir+inputDir);

			/* ----------------------------  create spool-out directory and the #numWorkers of files --------------------------------*/
			File spoolDir = new File(this.storageDir, "spool-out");
			// delete directories and content if it exists
			if (spoolDir.exists()) {
				String [] files = spoolDir.list();
				for (String s : files) {
					File file = new File(spoolDir.getPath(), s);
					file.delete();
				}
			}
			// create the directory
			new File(this.storageDir, "spool-out").mkdir();
			// and the files
			synchronized (this) {
				if (numWorkers == 0) { numWorkers = 1; }
			}
			for (int f = 0; f<numWorkers; f++) {
				if (new File(this.storageDir, "spool-out/spool"+(f+1)+".txt").createNewFile()) { 
					System.out.println("runmap(1("+(f+1)+")): spool"+(f+1)+".txt created!"); 
				}
			}			
			// debugging
			System.out.println("runmap(2): spool-out dir and files created!");


			/* -------------------------------------- start thread pool for MAP --------------------------------------------------*/
			System.out.println("runmap(3): thread pool started...");
			WorkerMapThread wMap = new WorkerMapThread(this, sReader, this.storageDir, inputDir, job, numWorkers);
			Thread [] mapThreads = new Thread[numThreads];
			for (int t = 0; t<numThreads; t++) {
				mapThreads[t] = new Thread(wMap, "worker map thread"+(t+1));
				mapThreads[t].start();
			}

			/*------------------------- wait for all the threads in the pool to finish execution --------------------------------*/
			
			for (int t = 0; t<numThreads; t++) {
				mapThreads[t].join();
			}
			System.out.println("runmap(4): thread pool finished execution!");

			/* -------------------------- /pushdata from spool-out dir to corresponding workers -------------------------------- */

			for (int w = 0; w<numWorkers; w++) {
				pushData(workerIPPORT[w], w);
			}
			System.out.println("runmap(5): Pushed data to the corresponding workers!");
			
			/* ------------------------------------- wait for /pushdata from all workers -------------------------------------------*/
			
			System.out.println("runmap(6): Waiting until all /pushdata received...");
			synchronized (Lock) {
				while (!this.pushDataReceived) {
//					Thread.sleep(10);
					try { Lock.wait(); } 
					catch (InterruptedException e) { break; }
				}
			}
			
			System.out.println("runmap(7): /pushdata from all the workers received!");
//			System.out.println("runmap: pushDataCounter: "+pushDataCounter);
//			System.out.println("runmap: numWorkers: "+this.numWorkers);
//			System.out.println("runmap: pushDataRecieved: "+pushDataReceived);
			
			/* -------------------- change status to 'waiting' to wait for /runreduce & send /workerstatus ---------------------------*/
			synchronized(this) {
				this.wsDS.setStatus(Status.waiting);
			}
			this.wsThread.interrupt();
			
			// reset
			synchronized (this) {
	 			this.pushDataCounter = 0; //reset
				this.pushDataReceived = false; // reset 
			}
			
			System.out.println("runmap(8): DONE! status: \"waiting\". Notified Master!");

		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			System.err.println("Error loading class "+jobStr);
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		// debugging
		System.out.println("------------------");
		System.out.println("job: "+jobStr);
		System.out.println("inputDir: "+inputDir);
		System.out.println("numThreads: "+numThreads);
		System.out.println("numWorkers: "+numWorkers);
		for (int i = 0; i<numWorkers; i++) {
			System.out.println("worker"+(i+1)+": "+workerIPPORT[i]);
		}
	}
	
	/*============================================================= runReduce() ============================================================ */
	
	private void runReduce(HttpServletRequest request) throws IOException {
		
		System.out.println("-----------------------------------------------------------");
		System.out.println("Received /runreduce [7]");
		
					
		/* -------------------------------------------------- read in the parameters -------------------------------------------------*/
		
		String jobStr = request.getParameter("job");
		String outputDir = request.getParameter("output");
		String numThreadsStr = request.getParameter("numThreads");
		int numThreads = 0;
		try { numThreads = Integer.parseInt(numThreadsStr); }
		catch (Exception e) {}
		
		// update status
		synchronized (this) {
			this.wsDS.setStatus(Status.reducing);
			this.wsDS.setJob(jobStr);
			this.wsDS.setKeysRead(0);
			this.wsDS.setKeysWritten(0);
			this.keysRead = 0;
			this.keysWritten = 0;
		}
		
		FileChannel c1 = null;
		FileChannel c = null;
		try {
			/* ---------------------------------- instantiate the job (WordCount) class ---------------------------------------------*/
			Object job = Class.forName(jobStr).newInstance();
			
			/* ---------------------------------------------- sort spool in-----------------------------------------------------------*/
			
			// merge all files in spool-in directory into one file -> spool1.txt
			c1 = new FileOutputStream(this.storageDir+"spool-in/spool1.txt", true).getChannel();	
			int numFiles = new File(this.storageDir, "spool-in").listFiles().length;
			
			for (int i = 1; i<numFiles; i++) {
				c = new FileInputStream(this.storageDir+"spool-in/spool"+(i+1)+".txt").getChannel();
				c1.transferFrom(c, c1.size(), c.size());
				// delete the rest of the files
				if (new File(this.storageDir, "spool-in/spool"+(i+1)+".txt").delete()) { System.out.println("spool"+(i+1)+" deleted!"); }
			}
			System.out.println("runreduce(1): Files in spool-in directory merged!");
			
			// sort the file
			String fileName = new File(this.storageDir, "spool-in/spool1.txt").getAbsolutePath();
			Process process = Runtime.getRuntime().exec("sort -b -o "+fileName+" "+fileName);
			// wait for the process to be completed and destory it to prevent any dangling open file decriptors
			process.waitFor();
			process.destroy();
			System.out.println("runreduce(2): Sorting done!");
			
			/* -------------------------- create output directory (delete old) and result.txt ---------------------------------------*/
			
			// delete content within directory if it exists
			File outDir = new File (this.storageDir, outputDir);
			if (outDir.exists()) {
				String [] files = outDir.list();
				for (String s : files) {
					new File(outDir.getPath(), s).delete();
				}
				outDir.delete();
				System.out.println("runreduce(3a): Output dir exists! Deleted output directory and contents within!");
			}
			// create output directory
			if (outDir.mkdir()) { System.out.println("runreduce(3). Output directory created successfully!"); }
			
			// create output file: result.txt
			File resultFile = new File (outDir.getAbsolutePath(), "result.txt");
			if (resultFile.createNewFile()) {
				System.out.println("runreduce(4): Output file \"result.txt\" created successfully!");
			}
			
			/* -------------------------------------- start thread pool for REDUCE --------------------------------------------------*/
			System.out.println("runreduce(5): thread pool started...");
			
			SyncReaderReduce sReader = new SyncReaderReduce(this.storageDir+"spool-in");
			WorkerReduceThread wReduce = new WorkerReduceThread(this, sReader, resultFile, job);
			Thread [] reduceThreads = new Thread[numThreads];
			for (int t = 0; t<numThreads; t++) {
				reduceThreads[t] = new Thread(wReduce, "worker reduce thread"+(t+1));
				reduceThreads[t].start();
			}
			
			/*------------------------- wait for all the threads in the pool to finish execution --------------------------------*/
			
			for (int t = 0; t<numThreads; t++) {
				reduceThreads[t].join();
			}
			System.out.println("runreduce(6): thread pool finished execution!");

			
			/* ------------------------------ change status to 'idle' & send /workerstatus -------------------------------------*/
			
			synchronized (this) {
				this.spoolInIndex = 1;
				this.wsDS.setStatus(Status.idle);
				this.wsDS.setKeysRead(0);
				this.keysRead = 0;
			}
			this.wsThread.interrupt();
			
			System.out.println("runreduce(7): Done! status=\"idle\". Notified Master! ");
			
//			// testing
//			File f = new File (this.storageDir, "spool-in/test.txt");
//			BufferedReader br = null;
//			try { 
//				br = new BufferedReader(new FileReader(f)); 
//				String currentLine;
//				while ((currentLine = br.readLine()) != null) {
//					try { 
//						ArrayList<String> values = new ArrayList<String>();
//
//						// read first line
//						String [] temp = currentLine.split("\\t");
//						String prevKey = temp[0].trim();
//						values.add(temp[1].trim());
//
//						// mark pointer
//						br.mark((int) f.length());
//						// read next line
//						String nextLine;
//						try {
//							if ((nextLine = br.readLine()) != null) {
//								temp = nextLine.split("\\t");
//								String key = temp[0].trim();
//
//								// add all similar keys
//								while (prevKey.equals(key)) {
//									values.add(temp[1].trim());
//									br.mark((int) f.length());
//
//									if ((nextLine = br.readLine()) != null) {
//										temp = nextLine.split("\\t");
//										key = temp[0].trim();
//									}
//									else { break; }
//								} // while similar keys
//							}
//						} catch (Exception e) { System.err.println("Coudln't read line(inner loop)! Check delimiter!"); }
//
//						br.reset();
//						System.out.print(prevKey+": ");
//						for (String s : values) {
//							System.out.print(s+"|");
//						}
//						System.out.println("");
//
//					} catch (Exception e) { System.err.println("Coudln't read line! Check delimiter!"); } // ignore lines which don't have a delimiter
//				}
//
//			
//			}
//			catch (FileNotFoundException e) {
//				System.err.println("File not found!"); e.printStackTrace(); 
//			} finally {
//				if (br!=null) { br.close(); }
//			}
//			
			

			
			
		} catch (InstantiationException | IllegalAccessException e ) {
			System.err.println("Error loading class "+jobStr);
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.err.println("Class cannot be found! "+jobStr);
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			System.err.println("File not found! Error reading from spool-in directory!");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error while merging files!");
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (c1 != null) { c1.close(); }
			if (c != null) { c.close(); }
		}
		
		
		// debugging
		System.out.println("------------------");
		System.out.println("job: "+jobStr);
		System.out.println("outputDir: "+outputDir);
		System.out.println("numThreads: "+numThreads);
	}
	
	/*============================================================= pushData() ============================================================ */
	
	// pushes the data from the spool out directory to the corresponding worker
	private void pushData(String workerIPPORT, int w) {
		try {

			URL url = new URL("http://"+ workerIPPORT +"/pushdata");
			HTTPClient client = new HTTPClient(url);

			// read the file into the body
			File f = new File(this.storageDir, "spool-out/spool"+(w+1)+".txt");
			client.readFileIntoBody(f);

			// send request
			client.sendRequest("POST", false);

//			System.out.println("/pushdata sent to "+workerIPPORT);

		} catch (MalformedURLException e) {
			System.err.println("Invalid URL!");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Could not read file!");
			e.printStackTrace();
		} catch (Exception e) {
			System.err.println("Error in sending request to Master!");
			e.printStackTrace();
		}
	}
	
	/*======================================================= receivePushData() ============================================================ */
	
	private void receivePushData(HttpServletRequest request) throws IOException {
		
		System.out.println("-----------------------------------------------------------");
		System.out.println("Received /pushdata [1]");
		
		// get the file to spool into (synchronized)
		File f = wsSync.createSpoolInFile();

		// read the body and spool in
		BufferedReader br = null;
		FileWriter fw = null;
		try {
			fw = new FileWriter(f);
			br = request.getReader();
			String line;
			while ((line=br.readLine()) != null) {
				fw.write(line);
				fw.write("\r\n");
			}
			
		} catch (IOException e) {
			System.err.println("Error while spooling in!");
			e.printStackTrace();
		} finally {
			if (br != null) { br.close(); }
			if (fw != null) { fw.close(); }			
		}
//		System.out.println(Thread.currentThread().getName()+" spooled in!");
		wsSync.incrementPushCounter();
//		System.out.println("receivePushData: pushDataCounter: "+pushDataCounter);
//		System.out.println("receivePushData: numWorkers: "+this.numWorkers);
//		System.out.println("receivePushData: pushDataRecieved: "+pushDataReceived);
		System.out.println("pushdata(1): Spooled in successfully!");
	}
	
//	private synchronized void incrementPushCounter() {
//		this.pushDataCounter++;
//		if (this.pushDataCounter == this.numWorkers) {
//			this.pushDataReceived = true;
//		}
//	}
//	
	
	/*============================================================= createSpoolInFile() ============================================================ */
	
//	private synchronized File createSpoolInFile(){
//		
//		// create spool-in dir
//		File spoolInDir = new File(this.storageDir, "spool-in");
//		// for new /runmap
//		if (this.spoolInIndex == 1) {
//			// if exists from before, delete contents
//			if (spoolInDir.exists()) {
//				// delete 
//				String [] files = spoolInDir.list();
//				for (String s : files) {
//					File file = new File(spoolInDir.getPath(), s);
//					file.delete();
//				}
//			}
//			// create a new spool-in directory
//			new File(this.storageDir, "spool-in").mkdir();
//		}
//
//		// create new file and return 
//		File f = new File(this.storageDir, "spool-in/spool"+(this.spoolInIndex)+".txt");
//		this.spoolInIndex++;
//		
////		// testing
////		System.out.println(Thread.currentThread().getName()+" is spooling in...");
////		Thread.sleep(10000);
//		
//		return f;
//	}
	
	/*============================================================= doGet() ============================================================ */

	public void doGet(HttpServletRequest request, HttpServletResponse response)  throws IOException {
		
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();

		System.out.println("GET request received!");
		System.out.println(request.getServletPath());

		out.println("<html><head><title>Worker</title></head>");
		out.println("<body>Hi, I am the worker! The WorkerStatusThread should hopefully be running!</body></html>");
		
//		System.out.println("staticCount: "+staticCount);
//		System.out.println("count: "+count);
//		System.out.println("storagedir: " + this.storageDir);
//		System.out.println("master host:ip :: " + this.masterHost+":"+this.masterPort);
	}
}

