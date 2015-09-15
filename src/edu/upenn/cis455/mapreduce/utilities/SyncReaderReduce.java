package edu.upenn.cis455.mapreduce.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/* @author Dhruva Kumar */

public class SyncReaderReduce {
	
	File ipDir;
	File [] listFiles;
	int fileInd;
	String currentLine;
	BufferedReader br;
	volatile boolean done;
	int maxCurrentFileSize = 100000;
	int currentFileSize;
	
	
	public SyncReaderReduce(String absInputDir) {
		this.ipDir = new File(absInputDir);
		this.listFiles = ipDir.listFiles();
		this.fileInd = 0;
		this.done = false;
		this.currentFileSize = this.maxCurrentFileSize;
		
		// initialize reader: do it in the calling program?
		this.initReader();
	}
	
	public boolean doneReading() {
		return this.done;
	}
	// set up the buffered reader
	public synchronized void initReader(){
		
		// if all the files have been read, update variable, close readers and quit
		if (this.fileInd == this.listFiles.length) {
			this.done = true;
			// close bufferedReader and fileReader
			if (this.br != null) {
				try { br.close(); System.out.println("SyncReaderReduce: File closed!"); } 
				catch (IOException e) { e.printStackTrace(); }
			}
			return;
		}
		
		if (this.listFiles[this.fileInd].isFile()) {
			
			try { 
				this.br = new BufferedReader(new FileReader(this.listFiles[this.fileInd])); 
				// setting mark setAheadLimit as the file size (any better idea?)
				this.currentFileSize = (int) this.listFiles[this.fileInd].length();
				
				if (this.currentFileSize < this.maxCurrentFileSize) { this.currentFileSize = this.maxCurrentFileSize; }
			} 
			// file not found. move on to the next file
			catch (FileNotFoundException e) {
				System.err.println("File not found!"); e.printStackTrace(); 
				this.fileInd++;
				initReader(); 
			}	
		} 
		// if it's not a file, move on to the next file
		else {
			this.fileInd++;
			System.out.println("thread: incremeneted!");
			initReader();
		}
	}
	
	public synchronized MyTupleMap read() throws InterruptedException, IOException {
		
		// if reading is done, quit
		if (this.done) {
			return null;
		}
		
		// return all similar keys and concatenate their values
		if ((this.currentLine = this.br.readLine()) != null) {
			try { 
				ArrayList<String> values = new ArrayList<String>();
				
				// read first line
				String [] temp = this.currentLine.split("\\t");
				String prevKey = temp[0].trim();
				values.add(temp[1].trim());
				
				// mark pointer
				this.br.mark(this.currentFileSize);
				// read next line
				String nextLine;
				try {
					if ((nextLine = this.br.readLine()) != null) {
						temp = nextLine.split("\\t");
						String key = temp[0].trim();
						
						// add all similar keys
						while (prevKey.equals(key)) {
							values.add(temp[1].trim());
							this.br.mark(this.currentFileSize);
							
							if ((nextLine = this.br.readLine()) != null) {
								temp = nextLine.split("\\t");
								key = temp[0].trim();
							} 
							else { break; }
						} // while similar keys
					}
				} catch (Exception e) { System.err.println("Coudln't read line(inner loop)! Check delimiter!"); }
				
				this.br.reset();
				return new MyTupleMap(prevKey, values.toArray(new String[values.size()]));

			} catch (Exception e) { System.err.println("Coudln't read line! Check delimiter! Here boss!"+this.currentFileSize);  e.printStackTrace(); } // ignore lines which don't have a delimiter
		} 
		// finished reading file. go to the next.
		else {
			System.out.println("thread2: incremeneted!");
			this.fileInd++;
			initReader();
		}
		
		return null;
		
	} // end read

}
