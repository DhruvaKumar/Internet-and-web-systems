# Map-reduce
Assignments for CIS 455/555 (http://www.cis.upenn.edu/~cis455/assignments.html)

Map reduce framework written in Java to run map reduce jobs. 

Description: <br />
• Created a distributed and multithreaded framework (similar to Apache Hadoop) to run map reduce jobs using java servlets. <br />
• Consisted of a master servlet where the map reduce job and its parameters were submitted to multiple workers via a RESTful architecture. <br />
• Each worker had its own thread pool and the workers coordinated with each other to run the map reduce job. <br />
• The map phase consisted of each worker reading the input files from its local directory using a thread pool. <br />
• The intermediate keys were shuffled by hashing it to the corresponding worker. SHA-1 was used for hashing. <br />
• The workers coordinated with each other to start the reduce phase once the mapping was over.  <br />
• A simple UI was created for the Master to instantiate map reduce jobs. <br />
⇨ Programming platform: Java
