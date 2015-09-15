package edu.upenn.cis455.mapreduce.utilities;

import java.util.Date;

/*
 * @author Dhruva Kumar
 * Data structure created to store the information of the worker status
 */

public class WorkerStatusDS {
	// String status;
	// enum Status {
	// mapping,
	// waiting,
	// reducing,
	// idle
	// }
	Status status;
	String job;
	int keysRead;
	int keysWritten;
	Date lastReceived;
	int portListen;

	// for the Master
	public WorkerStatusDS(Status status, String job, int keysRead,
			int keysWritten, Date lastReceived) {
		super();
		this.status = status;
		this.job = job;
		this.keysRead = keysRead;
		this.keysWritten = keysWritten;
		this.lastReceived = lastReceived;
	}

	// for the Worker
	public WorkerStatusDS(int portListen, Status status, String job,
			int keysRead, int keysWritten) {
		super();
		this.portListen = portListen;
		this.status = status;
		this.job = job;
		this.keysRead = keysRead;
		this.keysWritten = keysWritten;
	}

	public int getPortListen() {
		return portListen;
	}

	public void setPortListen(int portListen) {
		this.portListen = portListen;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public String getJob() {
		return job;
	}

	public void setJob(String job) {
		this.job = job;
	}

	public int getKeysRead() {
		return keysRead;
	}

	public void setKeysRead(int keysRead) {
		this.keysRead = keysRead;
	}

	public int getKeysWritten() {
		return keysWritten;
	}

	public void setKeysWritten(int keysWritten) {
		this.keysWritten = keysWritten;
	}

	public Date getLastReceived() {
		return lastReceived;
	}

	public void setLastReceived(Date lastReceived) {
		this.lastReceived = lastReceived;
	}

}
