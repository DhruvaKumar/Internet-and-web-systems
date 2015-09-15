package edu.upenn.cis455.mapreduce.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/* @author Dhruva Kumar */

public class SyncReader {
	
	File ipDir;
	File [] listFiles;
	int fileInd;
	String currentLine;
	BufferedReader br;
	boolean done;
	
	public SyncReader(String absInputDir) {
		this.ipDir = new File(absInputDir);
		this.listFiles = ipDir.listFiles();
		this.fileInd = 0;
		this.done = false;
		
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
				try { br.close();  System.out.println("SyncReader: File closed!"); } 
				catch (IOException e) { e.printStackTrace(); }
			}
			return;
		}
		
		if (this.listFiles[this.fileInd].isFile()) {
			
			try { this.br = new BufferedReader(new FileReader(this.listFiles[this.fileInd])); } 
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
			initReader();
		}
	}
	
	public synchronized MyTuple read() throws InterruptedException, IOException {
		
		// if reading is done, quit
		if (this.done) {
			return null;
		}
		
		// return each line/key-value pair
		if ((this.currentLine = this.br.readLine()) != null) {
			try { 
				String [] temp = this.currentLine.split("\\t");
				return new MyTuple(temp[0].trim(), temp[1].trim());
				
			} catch (Exception e) { System.err.println("Coudln't read line! Check delimiter!"); } // ignore lines which don't have a delimiter
		} 
		// finished reading file. go to the next.
		else {
			this.fileInd++;
			initReader();
		}
		
		return null;
		
	} // end read

}
