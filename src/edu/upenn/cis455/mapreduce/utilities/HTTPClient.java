package edu.upenn.cis455.mapreduce.utilities;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;
import java.util.TimeZone;

/* @author Dhruva Kumar */

public class HTTPClient {

	String urlString;
	URL url;
	InputStream in;
	OutputStream os;
	HashMap<String, String> response;
	HashMap<String, String> parameter;
	byte[] bodyBytes;
	HashMap<String, String> headers;
//	InputStream inFile;

	public HTTPClient() {
		this.parameter = new HashMap<String, String>();
		this.headers = new HashMap<String, String>();
		this.bodyBytes = null;
	}

	public HTTPClient(URL url) {
		this.url = url;
		this.parameter = new HashMap<String, String>();
		this.headers = new HashMap<String, String>();
		this.bodyBytes = null;
	}

	// getters
	public HashMap<String, String> getResponse() {
		return this.response;
	}

	public void buildURL(String urlStr) throws MalformedURLException {
		this.urlString = urlStr;
		this.url = new URL(this.urlString);
	}

	// setters
	public void setParameter(String key, String value) {
		this.parameter.put(key, value);
	}
	public void setHeader(String key, String value) {
		this.headers.put(key, value);
	}
//	public void setInputStreamFile(InputStream inFile) {
//		this.inFile = inFile;
//	}

	public void sendRequest(String method, boolean readResponse) throws Exception {
		
		// currently support GET & HEAD & POST
		if ( !(method.equals("GET") || method.equals("HEAD") || method.equals("POST")) ) {
			throw new Exception("Unsupported method! Supports only GET & HEAD & POST");
		}

		// Open a socket connection & get i/o streams
		int portNo = this.url.getPort();
		if (portNo == -1)
			portNo = url.getDefaultPort();

		try (Socket clientSoc = new Socket(this.url.getHost(), portNo);
				InputStream in = clientSoc.getInputStream();
				OutputStream os = clientSoc.getOutputStream();) {
			this.in = in;
			this.os = os;

			// Send request
			
			// url
			String urlPath = this.url.getPath();
			
			// check for query parameters and append to path if present
			if (!parameter.isEmpty()) {
				StringBuffer querySB = new StringBuffer();
				boolean firstTime = true;
				for (String key : parameter.keySet()) {
					if (firstTime) {
						querySB.append(key + "=" + parameter.get(key));
						firstTime = false;
					} else {
						querySB.append("&" + key + "=" + parameter.get(key));
					}
				}
				//  GET: append to URL | POST: send it in body
				if (method.equals("GET")) { urlPath = urlPath + "?" + querySB.toString(); }
				else if (method.equals("POST")) { this.bodyBytes = querySB.toString().getBytes(); }
				
//				System.out.println("query: "+querySB.toString());
			}

			// debugging
//			System.out.println("URL Path: " + urlPath);
			

			if (this.url.getPath().isEmpty()) {
				urlPath = "/";
			}
			//method
			this.os.write((method + " " + urlPath + " HTTP/1.0\r\n").getBytes());
			// user-agent
			this.os.write("User-Agent: cis455client\r\n".getBytes());
			// content-length
			if (this.bodyBytes != null) {
				this.os.write(("Content-Length: "+this.bodyBytes.length+"\r\n").getBytes());
			}
			// add any extra headers (if specified by the user) 
			if (!this.headers.isEmpty()) {
				for (String key : this.headers.keySet()) {
					this.os.write((key + ": " + this.headers.get(key)+"\r\n").getBytes());
				}
			}
			
			this.os.write(("\r\n").getBytes());
			
			// body
			if (this.bodyBytes != null) {
				this.os.write(this.bodyBytes);
			}
			this.os.flush();

			
			
			// Reads response
			if (readResponse) {
				readResponse();
			}
			
		}
	}

	public void readFileIntoBody(File f) {
		
		try (BufferedInputStream fis = new BufferedInputStream(new FileInputStream(f))) {
			this.bodyBytes = new byte[(int) f.length()];
			fis.read(this.bodyBytes);
		} catch (IOException e) {
			System.err.println("Error in reading file!");
			e.printStackTrace();
		} 
	}
	
	public String getDate(long date) {
		Date d = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat(
				"EEE, d MMM yyyy hh:mm:ss z");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

		return sdf.format(d);
	}

	// reads the reponse for a request into the hashmap
	public void readResponse() throws IOException {
		this.response = new HashMap<String, String>();
		StringBuffer sBuffer = new StringBuffer();
		int singleByte;

		// read headers
		while (true) {
			// System.out.println(sBuffer.toString());
			if (sBuffer.length() > 1000000) {
				break;
			}
			sBuffer.append((char) (singleByte = this.in.read())); // System.out.println("Appended:"+sBuffer);
			if ((char) singleByte == '\r') { // if we've got a '\r'
				sBuffer.append((char) this.in.read()); // then write '\n'
				singleByte = this.in.read(); // read the next char;
				if (singleByte == '\r') { // if it's another '\r'
					sBuffer.append((char) this.in.read()); // write the '\n'
					break;
				} else {
					sBuffer.append((char) singleByte);
				}
			}
		}
		// // debugging
		// // System.out.println("---------------------------------");
		// System.out.println("Response from server: ");
		// System.out.println(sBuffer.toString());
		// // System.out.println("---------------------------------");

		// store headers into hashmap
		boolean firstLine = true;
		Scanner scanner = new Scanner(sBuffer.toString());
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			if (firstLine) {
				try {
					String[] fl = line.split("\\s+");
					this.response.put("ver", fl[0]); // HTTP/1.1
					this.response.put("resp-code", fl[1]); // 200
					this.response.put("resp-msg", fl[2]); // OK
				} catch (Exception e) {
					this.response.put("ver", null);
					this.response.put("resp-code", null);
					this.response.put("resp-msg", null);
				}
				firstLine = false;
			} else {
				try {
					String[] l = line.split(":");
					this.response.put(l[0].toLowerCase().trim(), l[1].trim());
				} catch (Exception e) {
					break;
				}
			}
		}
		scanner.close();

		// store body too
		this.response.put("body", convertStreamToString(this.in));

		// // debugging
		// System.out.println("---------------------------------");
		// System.out.println("Response hashmap: ");
		// for (String key : this.response.keySet()) {
		// System.out.println(key + ": " + this.response.get(key));
		// }
		// System.out.println("---------------------------------");

	}

	static String convertStreamToString(InputStream is) {
		Scanner s = new Scanner(is).useDelimiter("\\A");
		return s.hasNext() ? s.next() : "";
	}

}

// Open a socket connection & get i/o streams
// Send request
// Read in response from server
