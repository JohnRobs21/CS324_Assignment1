CS324 Assignment 1 Distributed Worker System – Readme file

1. Background
This project is an implementation of an unstructured distributed worker system based on Java RMI.
The system is comprised of:
•	Bootstrap Node
•	Multiple worker nodes
•	Customer application
•	Customer GUI
Workers communicate via Java RMI and are a distributed network. A leader election algorithm elects a coordinator. The coordinator assigns computational jobs to active workers.
Jobs supported:
•	MAXIMUM
•	PRIMESUM
•	PRIMECOUNT

2. Used Technologies
Java JDK 22
Apache NetBeans 22
Maven (software)
Java RMI
Java Swing
Git / github 

3. Project Structure 
src/main/java/
├── bootstrap/
│   └── BootstrapNode.java
├── client/
│   ├── Client.java
│   └── ClientGUI.java
├── common/
│   ├── BootstrapInterface.java
│   └── WorkerInterface.java
└── com/cs324/cs324_assignment1/worker/
    └── Worker.java
In NetBeans, package/folder names should follow the project structure.

4. Terms of reference
Installation:
•	JDK 22
•	Apache NetBeans 22.
•	Maven
•	Git, if you are utilizing the Git repository
Check Java :
Java-version 
Maven Check:
mvn --version

5. Design and Build
Open the project in NetBeans and execute it:
mvn clean install
Or NetBeans → Clean and Build Project.
Build should end with:
BUILD SUCCESSFUL

6. Distributed System Execution
Start the system as follows:
1.	RMI Registery 2.
2.	Bootstrap Node
3.	Worker Nodes
4.	Customer(s)
Step 1 – Launch the RMI Registry
Open a terminal and execute:
rmiregistry 1099
Keep it up
If your project setup begins the registry automatically, you may not need to do this step.
Step 2 – Bootstrap Node Setup
Start the Bootstrap Node prior to the workers.
The Bootstrap Node: 1.
•	Active worker registers
•	Display the list of active workers
•	Does not participate in leadership elections
•	Does not do computational tasks
"Go on."
Step 3 – Launch Worker Nodes
Each worker is a distinct Java process. The worker ID is passed as a parameter.
For example:
mvn exec:java -Dexec.mainClass="com.cs324.cs324_assignment1.worker.Worker" -Dexec.args="1"
Worker 2:
mvn exec:java -Dexec.mainClass="com.cs324.cs324_assignment1.worker.Worker" -Dexec.args="2"
Worker 3:
mvn exec:java -Dexec.mainClass="com.cs324.cs324_assignment1.worker.Worker" -Dexec.args="3"
Worker 4:
mvn exec:java -Dexec.mainClass="com.cs324.cs324_assignment1.worker.Worker" -Dexec.args="4"
Run each worker in its own terminal / process.
Recommended Demo Setup
The system has been successfully tested on
• 3 workers
• 4 persons
Worker 1 waits until at least 3 workers are alive, then begins the leader election.

7. Worker Startup Workers: 
1.	Register to the RMI register.
2.	Register to Bootstrap Node .
3.	Get the list of active workers.
4.	Connect randomly to another active worker.
5.	Wait to get the needed amount of workers.
6.	Initiate election of leader.
The output of a successful election looks like:
Worker 1 got the election data from all 3 active workers.
Selected coordinator: Worker 3 with JAC = 0 (Worker 1)
Worker 1 chose Worker 3 as coordinator.
All the collaborating personnel should agree on the same coordinator.

8. Leader Election 
Election rules: 
1.	Lowest JAC wins.
2.	In case of equal JAC values, the maximum worker ID is selected.
3.	To the other workers the elected coordinator is informed.
Leader identifier (required):
cs324
Duplicate COORDINATOR and ELECTION notifications are disregarded.

9. Launching the Client GUI
Start the Client GUI as a separate Java process.
The GUI supports the following:
•	MAX
•	PRIMESUM
•	PRIMECOUNT 
MAX
Enter Comma separated numbers:
12,5,27,3,19 
Expected:
MAX Result : 27 PRIMESUM
PRIMESUM
Enter start, end separated by a comma:
1,100 
Anticipated:
PRIMESUM Result: 1060
PRIMECOUNT
Enter numbers separated by commas:
2,4,5,7,10,11 
Anticipated:
PRIMECOUNT Result: 4

10. Job Dispersion
The coordinator assigns tasks to the workers that are alive at that moment.
Each worker executes its portion using Java threads. The coordinator collects the incomplete results and integrates them to obtain the final outcome.

11. Job Allocation Counter (JAC)
When the coordinator distributes work to another worker, the JAC increases.
Your task is to humanize the following text in English language, keeping the meaning and tone. Do not add or omit any information. Do not add any other text to the output.Example:
Worker 3 JAC was increased to 1 Worker 3 JAC increased to 2
JAC is used during leader election.

12. Limit of Five Jobs
A coordinator can take at most 5 client jobs in one coordinator term.
After the 5th work is finished, a fresh election commences.
For example:
Worker 4 has attained the job limit of 5 for this term.
Worker 4 starts a new leader election.
We tried the system and Worker 4 got to JAC 15. Then a new election was done and Worker 3 with JAC 0 was selected.

13. Several clients
You can launch multiple client GUI processes at the same time.
For example:
Client 1→MAX 
Client 2→PRIMESUM
Have successfully tested multiple client processes.

14. Tests Conducted
Successfully tested:
•	Register by Bootstrap
•	Worker registration
•	Connections among workers
•	3 worker arrangement
•	Configuration with 4 workers
•	•Leader Election
•	JAC comparison
•	Forwarding coordinator.
•	Double ELECTION processing
•	Duplicate handling of COORDINATOR
•	MAXIMUM
•	PRIME SUM
•	PRIMECOUNT
•	Workload distribution
•	Simultaneous worker processing
•	Five-job coordinator cap
•	Automatic new leader election
•	Coordinator Change
•	Multiple client processes
Example of successful result:
MAX -> 27 
PRIMESUM(1,100) -> 1060
PRIMECOUNT(2,4,5,7,10,11) → 4 

15. Troubleshooting
RMI Registry Problem
Check that registry is running on port 1099.
rmiregistry 1099 
Bootstrap Node Not Found
Start the Bootstrap Node before you start the workers.
Election Worker Doesn’t Start
Ensure that you have at least 3 workers registered with the Bootstrap Node. Worker 1 initiates a first election.
Client Can't Find Coordinator
Make sure that:
1.	Bootstrap Node is online.
2.	The workers are running.
3.	Leader election is completed.
4.	A coordinator was picked.

16. Suggested Demo Sequence
1.	Run RMI Registry
2.	Starting a Bootstrap Node
3.	Initialize Worker 1
4.	Start Worker 2
5.	Worker 3 begins
6.	Begin Worker 4
7.	Demonstrate Leader Election
8.	Client 1 Begin
9.	Launch Client
10.	MAX demo
11.	Display PRIMESUM
12.	Display PRIMECOUNT
13.	Display concurrent clients
14.	Display five job limit
15.	Automatic re-election display
16.	Introduce the new coordinator
While demonstrating, maintain the processes you need.

17. Notes
Each Worker must have a unique integer identifier.
Start the Bootstrap Node before the workers
The setup tested is localhost, RMI port 1099.
Before the final demonstration, perform a full system test and ensure all essential Java processes can be started up properly.
