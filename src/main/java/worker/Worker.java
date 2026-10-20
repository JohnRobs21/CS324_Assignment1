package com.cs324.cs324_assignment1.worker;

import Interface.WorkerInterface;
import Interface.BootstrapInterface;

import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author gound
 */
public class Worker extends UnicastRemoteObject
        implements WorkerInterface {

    // Stores the unique ID of this worker.
    private int workerId;

    // Job Allocation Counter (JAC).
    private int jac = 0;
    
    // Number of jobs assigned during the current coordinator term.
    private int jobsAssigned = 0;

    // Maximum number of jobs a coordinator can assign in one term.
    private static final int MAX_JOBS_PER_TERM = 5;

    // Required identifier for the leader election system.
    private String leaderman = "cs324";

    // Stores the ID of the current coordinator.
    // -1 means no coordinator is currently active.
    private int coordinatorId = -1;

    // Stores election messages that this worker has already processed.
    private Set<String> processedElections = new HashSet<>();

    // Stores COORDINATOR messages that this worker has already processed.
    private Set<Integer> processedCoordinators = new HashSet<>();

    // Stores workers that this worker is directly connected to.
    // Key   = Worker ID
    // Value = RMI reference to the worker
    private Map<Integer, WorkerInterface> neighbours = new HashMap<>();

    // Stores workers participating in the election.
    private Map<Integer, Integer> electionWorkers =
            new HashMap<>();

    // Stores election data that has already been forwarded.
    // Key   = Election ID
    // Value = Set of Worker IDs
    private Map<String, Set<Integer>>
            forwardedElectionData = new HashMap<>();

    // Stores elections that have already been completed.
    private Set<String> finishedElections = new HashSet<>();

    
     // Constructor for the Worker.
    public Worker(int workerId) throws RemoteException {

        // Export the Worker object for Java RMI.
        super();

        this.workerId = workerId;
    }

    // Returns the ID of this worker.
    @Override
    public int getWorkerId() throws RemoteException {

        return workerId;
    }

    //Returns the current Job Allocation Counter (JAC).
    @Override
    public int getJac() throws RemoteException {

        return jac;
    }

     //Increments the Job Allocation Counter.
     
    public void incrementJac() {

        jac++;

        System.out.println(
                "Worker " + workerId
                + " JAC increased to " + jac
        );
    }

    /**
     * Assigns a job to another worker.
     *
     * Only the current coordinator can assign a job.
     *
     *  targetWorker the worker receiving the job
     *  jobName name of the job being assigned
     */
    public void assignJob(
            WorkerInterface targetWorker,
            String jobName)
            throws RemoteException {

        // Check whether this worker is the coordinator.
        if (coordinatorId != workerId) {

            System.out.println(
                    "Worker " + workerId
                    + " cannot assign job. "
                    + "It is not the coordinator."
            );

            return;
        }

        // Increase JAC because the coordinator
        // has assigned a job to another worker.
        if (targetWorker.getWorkerId() != workerId) {
            incrementJac();
        }
        
        // Count the job assigned during this coordinator term.
        jobsAssigned++;

        System.out.println(
                "Worker " + workerId
                + " has assigned "
                + jobsAssigned
                + " job(s) in this term."
        );
        
        // Start a new election after 5 jobs.
        if (jobsAssigned >= MAX_JOBS_PER_TERM) {

            System.out.println(
                    "Worker " + workerId
                    + " has reached the maximum of "
                    + MAX_JOBS_PER_TERM
                    + " jobs."
            );

            System.out.println(
                    "Worker " + workerId
                    + " is starting a new leader election."
            );

            jobsAssigned = 0;

            startElection();
        }

        System.out.println(
                "Worker " + workerId
                + " assigned job: "
                + jobName
        );

        // Send the job information to the target worker.
        targetWorker.receiveMessage(
                "Job assigned: " + jobName
        );

        // If the job is MAX, send the actual
        // calculation to the target worker.
        if (jobName.equalsIgnoreCase("MAX")) {

            int[] numbers = {
                12, 5, 27, 3, 19
            };

            int result =
                    targetWorker.processMaxJob(numbers);

            System.out.println(
                    "Coordinator Worker " + workerId
                    + " received MAX result = "
                    + result
            );
        }
    }

    
     //Calculates the maximum value in an unsorted list.
     // numbers list of integers
     //return largest value in the list
     
    public int calculateMax(int[] numbers) {

        // Check that the list is not empty.
        if (numbers == null || numbers.length == 0) {

            throw new IllegalArgumentException(
                    "Numbers list cannot be empty."
            );
        }

        int max = numbers[0];

        // Check every number in the list.
        for (int number : numbers) {

            if (number > max) {
                max = number;
            }
        }

        return max;
    }

    
     //Processes a MAX job.
    // list of numbers
     // maximum value
     
    @Override
    public int processMaxJob(int[] numbers)
            throws RemoteException {

        int result = calculateMax(numbers);

        System.out.println(
                "Worker " + workerId
                + " processed MAX job. Result = "
                + result
        );

        return result;
    }

    
      //Processes a PRIMESUM job.
     //  start starting value
     //  end ending value
     // return sum of prime numbers in the range
    @Override
    public int processPrimeSumJob(
            int start,
            int end)
            throws RemoteException {

        int sum = 0;

        for (int number = start;
                number <= end;
                number++) {

            if (isPrime(number)) {
                sum += number;
            }
        }

        System.out.println(
                "Worker " + workerId
                + " processed PRIMESUM job. Range = "
                + start + "-" + end
                + ", Result = " + sum
        );

        return sum;
    }

    
     //Processes a PRIMECOUNT job.
     //return number of prime numbers
    @Override
    public int processPrimeCountJob(
            int[] numbers)
            throws RemoteException {

        int count = 0;

        for (int number : numbers) {

            if (isPrime(number)) {
                count++;
            }
        }

        System.out.println(
                "Worker " + workerId
                + " processed PRIMECOUNT job. Count = "
                + count
        );

        return count;
    }

    
      //Checks whether a number is prime.   
     // number number to check
     //return true if prime, otherwise false
     
    private boolean isPrime(int number) {

        if (number < 2) {
            return false;
        }

        for (int i = 2;
                i <= Math.sqrt(number);
                i++) {

            if (number % i == 0) {
                return false;
            }
        }

        return true;
    }

    
     //Adds another worker as a direct neighbour.
     //  neighbourId ID of the neighbour worker
     //  neighbour remote reference to the neighbour
    @Override
    public void addNeighbour(
            int neighbourId,
            WorkerInterface neighbour)
            throws RemoteException {

        neighbours.put(
                neighbourId,
                neighbour
        );

        System.out.println(
                "Worker " + workerId
                + " connected to Worker "
                + neighbourId
        );
    }

    
     // Starts a new leader election.
     
    public void startElection()
            throws RemoteException {

        // Create a unique ID for this election.
        String electionId =
                "ELECTION-" + System.currentTimeMillis();

        // Clear old election information.
        electionWorkers.clear();

        System.out.println(
                "Worker " + workerId
                + " is starting election: "
                + electionId
        );

        // Process the election locally first.
        receiveElection(electionId);
    }

    /**
     * Selects the coordinator using the assignment election rules.
     *
     * Rule:
     * 1. Lowest JAC is selected.
     * 2. If JAC values are equal, highest Worker ID is selected.
     *
     * return Worker ID of the selected coordinator
     */
    public int selectCoordinator() {

        int selectedWorkerId = -1;
        int lowestJac = Integer.MAX_VALUE;

        for (Map.Entry<Integer, Integer> entry
                : electionWorkers.entrySet()) {

            int currentWorkerId = entry.getKey();
            int currentJac = entry.getValue();

            // Lowest JAC wins.
            if (currentJac < lowestJac) {

                lowestJac = currentJac;
                selectedWorkerId = currentWorkerId;
            }

            // Highest Worker ID wins when JAC is tied.
            else if (currentJac == lowestJac
                    && currentWorkerId > selectedWorkerId) {

                selectedWorkerId = currentWorkerId;
            }
        }

        System.out.println(
                "Worker " + workerId
                + " selected coordinator: Worker "
                + selectedWorkerId
                + " with JAC = "
                + lowestJac
        );

        return selectedWorkerId;
    }

    
      //Receives a normal message from another worker.
     
    @Override
    public void receiveMessage(String message)
            throws RemoteException {

        System.out.println(
                "Worker " + workerId
                + " received message: "
                + message
        );
    }

    /**
     * Receives an ELECTION message.
     *
     * Each election is processed only once.
     *
     * electionId unique ID of the election
     */
    @Override
    public void receiveElection(
            String electionId)
            throws RemoteException {

        // Ignore an ELECTION message that has already
        // been processed by this worker.
        if (processedElections.contains(electionId)) {

            System.out.println(
                    "Worker " + workerId
                    + " ignored duplicate ELECTION: "
                    + electionId
            );

            return;
        }

        // Mark this election as processed.
        processedElections.add(electionId);

        System.out.println(
                "Worker " + workerId
                + " processed ELECTION: "
                + electionId
        );

        // Store this worker's ID and JAC.
        electionWorkers.put(
                workerId,
                jac
        );

        // Get the election data that has already
        // been forwarded for this election.
        Set<Integer> forwardedWorkers =
                forwardedElectionData.computeIfAbsent(
                        electionId,
                        k -> new HashSet<>()
                );

        // Mark this worker's own data as forwarded.
        forwardedWorkers.add(workerId);

        System.out.println(
                "Worker " + workerId
                + " election data: JAC = "
                + jac
        );

        // Send this worker's election data to
        // all direct neighbours.
        for (Map.Entry<Integer, WorkerInterface> entry
                : neighbours.entrySet()) {

            int neighbourId = entry.getKey();
            WorkerInterface neighbour = entry.getValue();

            try {

                System.out.println(
                        "Worker " + workerId
                        + " sending election data to Worker "
                        + neighbourId
                );

                neighbour.receiveElectionData(
                        electionId,
                        workerId,
                        jac
                );

            } catch (RemoteException e) {

                System.out.println(
                        "Worker " + workerId
                        + " could not send election data to Worker "
                        + neighbourId
                );

                e.printStackTrace();
            }
        }

        // Forward the ELECTION message to all neighbours.
        for (Map.Entry<Integer, WorkerInterface> entry
                : neighbours.entrySet()) {

            int neighbourId = entry.getKey();
            WorkerInterface neighbour = entry.getValue();

            try {

                System.out.println(
                        "Worker " + workerId
                        + " forwarding ELECTION to Worker "
                        + neighbourId
                );

                neighbour.receiveElection(
                        electionId
                );

            } catch (RemoteException e) {

                System.out.println(
                        "Worker " + workerId
                        + " could not forward ELECTION to Worker "
                        + neighbourId
                );

                e.printStackTrace();
            }
        }
    }

    /**
     * Receives election information from another worker.
     *
     *  electionId unique ID of the election
     *  candidateId ID of the worker
     *  candidateJac JAC value of the worker
     */
    @Override
    public synchronized void receiveElectionData(
            String electionId,
            int candidateId,
            int candidateJac)
            throws RemoteException {

        // Get the set of candidate IDs already processed
        // for this election.
        Set<Integer> forwardedWorkers =
                forwardedElectionData.computeIfAbsent(
                        electionId,
                        k -> new HashSet<>()
                );

        // Ignore duplicate election data.
        if (forwardedWorkers.contains(candidateId)) {
            return;
        }

        // Store the candidate's ID and JAC.
        electionWorkers.put(candidateId,candidateJac);

        System.out.println(
                "Worker " + workerId
                + " received election data: Worker "
                + candidateId
                + ", JAC = "
                + candidateJac
        );

        // Mark this candidate as processed.
        forwardedWorkers.add(candidateId);

        
         //Forward the election data in a separate thread.
         //This prevents a chain of RMI calls from blocking
         //the worker that sent us the election data.
         
        new Thread(() -> {

            for (Map.Entry<Integer, WorkerInterface> entry
                    : neighbours.entrySet()) {

                int neighbourId = entry.getKey();
                WorkerInterface neighbour = entry.getValue();

                // Do not send the data directly back to
                // the worker that owns this candidate ID.
                if (neighbourId != candidateId) {

                    try {

                        System.out.println(
                                "Worker " + workerId
                                + " forwarding election data to Worker "
                                + neighbourId
                        );

                        neighbour.receiveElectionData(electionId,candidateId,candidateJac);

                    } catch (RemoteException e) {

                        System.out.println(
                                "Worker " + workerId
                                + " could not forward election data "
                                + "to Worker "
                                + neighbourId
                        );
                    }
                }
            }

        }).start();

         // Check whether all active workers have provided
         //their election data.
        
        new Thread(() -> {

            try {

                Registry registry = LocateRegistry.getRegistry(
                                "localhost",1099
                        );

                BootstrapInterface bootstrap =
                        (BootstrapInterface)
                        registry.lookup("BootstrapNode");

                WorkerInterface[] activeWorkers = bootstrap.getActiveWorkers();

                int activeWorkerCount = activeWorkers.length;

                if (electionWorkers.size()
                        >= activeWorkerCount) {

                    synchronized (finishedElections) {

                        if (!finishedElections.contains(electionId)) {

                            finishedElections.add(electionId);

                            System.out.println(
                                    "Worker " + workerId
                                    + " has received election data "
                                    + "from all "
                                    + activeWorkerCount
                                    + " active workers."
                            );

                            finishElection(electionId);
                        }
                    }
                }

            } catch (Exception e) {

                System.out.println(
                        "Worker " + workerId
                        + " could not check active workers."
                );

                e.printStackTrace();
            }

        }).start();
    }

    /**
     * Finishes collecting election information
     * and selects the coordinator.
     *
     * 
     */
    @Override
    public void finishElection(String electionId)
            throws RemoteException {

        System.out.println(
                "Worker " + workerId
                + " finished collecting election data for "
                + electionId
        );

        int selectedCoordinator = selectCoordinator();

        System.out.println(
                "Worker " + workerId
                + " selected Worker "
                + selectedCoordinator
                + " as coordinator."
        );

        announceCoordinator(selectedCoordinator);
    }

    /**
     * Receives a COORDINATOR message.
     *
     * coordinatorId ID of the elected coordinator
     */
    @Override
    public void receiveCoordinator(int coordinatorId)
            throws RemoteException {

        // Ignore duplicate coordinator messages.
        if (processedCoordinators.contains(coordinatorId)) {

            System.out.println(
                    "Worker " + workerId
                    + " ignored duplicate COORDINATOR message: "
                    + "Worker " + coordinatorId
            );

            return;
        }

        // Mark the coordinator message as processed.
        processedCoordinators.add(coordinatorId);

        // Store the current coordinator.
        this.coordinatorId = coordinatorId;

        System.out.println(
                "Worker " + workerId
                + " received COORDINATOR message: "
                + "Worker " + coordinatorId
                + " is the coordinator."
        );

        // Forward the coordinator message.
        for (WorkerInterface neighbour :
                neighbours.values()) {

            try {

                neighbour.receiveCoordinator(coordinatorId);

            } catch (RemoteException e) {

                System.out.println(
                        "Worker " + workerId
                        + " could not forward COORDINATOR "
                        + "message."
                );
            }
        }
    }

    /**
     * Announces the elected coordinator
     * to all neighbouring workers.
     *
     * @param coordinatorId ID of the elected coordinator
     */
    public void announceCoordinator(int coordinatorId)
            throws RemoteException {

        // Set the coordinator locally.
        this.coordinatorId = coordinatorId;

        // Mark this coordinator message as processed.
        processedCoordinators.add(coordinatorId);

        System.out.println(
                "Worker " + workerId
                + " announcing Worker "
                + coordinatorId
                + " as coordinator."
        );

        // Send the COORDINATOR message
        // to all direct neighbours.
        for (WorkerInterface neighbour :
                neighbours.values()) {

            try {

                neighbour.receiveCoordinator(coordinatorId);

            } catch (RemoteException e) {

                System.out.println(
                        "Worker " + workerId
                        + " could not send coordinator "
                        + "message."
                );
            }
        }
    }

    
     //Test for MAX, PRIMESUM and PRIMECOUNT.
     public void testMaxJob() {

        if (coordinatorId != workerId) {
            return;
        }

        try {

            // Find a worker to receive the jobs.
            for (WorkerInterface neighbour :
                    neighbours.values()) {

                // Test MAX job.
                System.out.println(
                        "Coordinator Worker " + workerId
                        + " is testing MAX job..."
                );

                assignJob(neighbour,"MAX");

                // Test PRIMESUM job.
                System.out.println(
                        "Coordinator Worker " + workerId
                        + " is testing PRIMESUM job..."
                );

                int primeSumResult = neighbour.processPrimeSumJob(1,10);

                System.out.println(
                        "Coordinator Worker " + workerId
                        + " received PRIMESUM result = "
                        + primeSumResult
                );

                // Test PRIMECOUNT job.
                System.out.println(
                        "Coordinator Worker " + workerId
                        + " is testing PRIMECOUNT job..."
                );

                int[] testNumbers = {
                    2, 4, 5, 7, 10, 11
                };

                int primeCountResult =
                        neighbour.processPrimeCountJob(
                                testNumbers
                        );

                System.out.println(
                        "Coordinator Worker " + workerId
                        + " received PRIMECOUNT result = "
                        + primeCountResult
                );

                break;
            }

        } catch (RemoteException e) {

            System.out.println(
                    "Could not assign job."
            );

            e.printStackTrace();
        }
    }

    /**
     * Divides a PRIMESUM job among all active workers.
     *
     * Example:
     * PRIMESUM(1, 1000) with 4 workers:
     *
     * Worker 1 -> 1-250
     * Worker 2 -> 251-500
     * Worker 3 -> 501-750
     * Worker 4 -> 751-1000
     *
     * The coordinator also processes one part itself.
     */
    public void assignPrimeSumJob(int start, int end)
            throws RemoteException {

        // Only the coordinator can assign jobs.
        if (coordinatorId != workerId) {

            System.out.println(
                    "Worker " + workerId
                    + " cannot assign PRIMESUM job. "
                    + "It is not the coordinator."
            );

            return;
        }
        
        // Count PRIMESUM as one job in the current coordinator term.
        jobsAssigned++;

        System.out.println(
                "Worker " + workerId
                + " has assigned "
                + jobsAssigned
                + " job(s) in this term."
        );

        // Connect to Bootstrap Node.
        Registry registry = LocateRegistry.getRegistry("localhost",1099);

        BootstrapInterface bootstrap;

        try {

            bootstrap = (BootstrapInterface)
                    registry.lookup("BootstrapNode");

        } catch (NotBoundException e) {

            System.out.println("Bootstrap Node could not be found.");

            return;
        }

        // Get all currently active workers.
        WorkerInterface[] activeWorkers = bootstrap.getActiveWorkers();

        if (activeWorkers.length == 0) {

            System.out.println("No active workers available.");

            return;
        }

        // Store active workers in a list.
        List<WorkerInterface> workers = new ArrayList<>();

        for (WorkerInterface activeWorker : activeWorkers) {

            workers.add(activeWorker);
        }

        // Sort workers by Worker ID.
        workers.sort(
                (workerA, workerB) -> {

                    try {

                        return Integer.compare(workerA.getWorkerId(),
                                workerB.getWorkerId()
                        );

                    } catch (RemoteException e) {
                        return 0;
                    }
                }
        );

        int workerCount = workers.size();

        int totalNumbers = end - start + 1;

        // Basic number of values per worker.
        int baseSize = totalNumbers / workerCount;

        // Remaining values.
        int remainder = totalNumbers % workerCount;

        int currentStart = start;

        // Store the total result.
        AtomicLong totalResult = new AtomicLong(0);

        // Count completed workers.
        AtomicInteger completedWorkers = new AtomicInteger(0);

        System.out.println(
                "Coordinator Worker " + workerId
                + " dividing PRIMESUM "
                + start + "-" + end
                + " among "
                + workerCount
                + " workers."
        );

        for (int i = 0;
                i < workerCount;
                i++) {

            // Give the first workers one extra
            // number when necessary.
            int currentSize = baseSize
                    + (i < remainder ? 1 : 0);

            int currentEnd = currentStart
                    + currentSize
                    - 1;

            WorkerInterface target = workers.get(i);

            int targetId = target.getWorkerId();

            System.out.println(
                    "Coordinator Worker " + workerId
                    + " assigning PRIMESUM range "
                    + currentStart
                    + "-"
                    + currentEnd
                    + " to Worker "
                    + targetId
            );

            // Increase JAC when assigning work
            // to another worker.
            if (targetId != workerId) {
                incrementJac();
            }

            // Store final values for the thread.
            final int rangeStart = currentStart;

            final int rangeEnd = currentEnd;

            new Thread(() -> {

                try {

                    int result;

                    // The coordinator processes its own
                    // portion locally.
                    if (targetId == workerId) {

                        result = processPrimeSumJob(
                                        rangeStart,
                                        rangeEnd
                                );

                    } else {

                        target.receiveMessage(
                                "PRIMESUM job assigned: "
                                + rangeStart
                                + "-"
                                + rangeEnd
                        );

                        result = target.processPrimeSumJob(rangeStart,rangeEnd);
                    }

                    totalResult.addAndGet(result);

                    int completed = completedWorkers.incrementAndGet();

                    System.out.println(
                            "Coordinator Worker "
                            + workerId
                            + " received PRIMESUM result "
                            + result
                            + " from Worker "
                            + targetId
                    );

                    // Once all workers have finished,
                    // display the final total.
                    if (completed == workerCount) {

                        System.out.println(
                                "================================"
                        );

                        System.out.println(
                                "PRIMESUM job completed."
                        );

                        System.out.println(
                                "Range: "
                                + start
                                + "-"
                                + end
                        );

                        System.out.println("Final PRIMESUM result = "+ totalResult.get());

                        System.out.println(
                                "================================"
                        );
                    }

                } catch (RemoteException e) {

                    System.out.println(
                            "Could not process PRIMESUM "
                            + "range "
                            + rangeStart
                            + "-"
                            + rangeEnd
                            + " on Worker "
                            + targetId
                    );

                    e.printStackTrace();
                }

            }).start();

            currentStart = currentEnd + 1;
        }
        
        // Start a new election after 5 jobs.
        if (jobsAssigned >= MAX_JOBS_PER_TERM) {

            System.out.println(
                    "Worker " + workerId
                    + " has reached the maximum of "
                    + MAX_JOBS_PER_TERM
                    + " jobs."
            );

            System.out.println(
                    "Worker " + workerId
                    + " is starting a new leader election."
            );

            jobsAssigned = 0;

            startElection();
        }
    }

    /**
     * Connects this worker to one randomly selected
     * active worker obtained from the Bootstrap Node.
     *
     * The current worker is excluded from the list.
     */
    private static void connectToRandomWorker(Worker worker,BootstrapInterface bootstrap)
            throws RemoteException {

        // Get all currently active workers.
        WorkerInterface[] activeWorkers = bootstrap.getActiveWorkers();

        // Store workers other than this worker.
        List<WorkerInterface> possibleWorkers = new ArrayList<>();

        for (WorkerInterface activeWorker : activeWorkers) {

            if (activeWorker.getWorkerId()
                    != worker.workerId) {

                possibleWorkers.add(activeWorker);
            }
        }

        // If no other worker exists, this worker
        // cannot create a connection yet.
        if (possibleWorkers.isEmpty()) {

            System.out.println(
                    "Worker " + worker.workerId
                    + " is currently the only active worker."
            );

            return;
        }

        // Select a random active worker.
        Random random = new Random();

        WorkerInterface selectedWorker = possibleWorkers.get(random.nextInt(
                                possibleWorkers.size()
                        )
                );

        int selectedWorkerId = selectedWorker.getWorkerId();

        // Add the selected worker as a neighbour.
        worker.addNeighbour(selectedWorkerId,selectedWorker);

        // Add this worker as a neighbour
        // of the selected worker.
        selectedWorker.addNeighbour(worker.workerId,worker);

        System.out.println(
                "Worker " + worker.workerId
                + " randomly connected to Worker "
                + selectedWorkerId
        );
    }

    /**
     * Waits until the required workers are registered
     * with the Bootstrap Node, then starts the election.
     */
    private void waitForWorkersAndStartElection() {

        new Thread(() -> {

            try {

                Registry registry = LocateRegistry.getRegistry("localhost",1099);

                BootstrapInterface bootstrap = (BootstrapInterface)
                        registry.lookup("BootstrapNode");

                while (true) {

                    WorkerInterface[] activeWorkers = bootstrap.getActiveWorkers();

                    System.out.println(
                            "Worker " + workerId
                            + " checking active workers: "
                            + activeWorkers.length
                    );

                    // Wait for at least 3 workers before starting the election.
                    if (activeWorkers.length >= 3) {

                        System.out.println(
                                "Worker " + workerId
                                + " detected enough workers."
                        );

                        System.out.println(
                                "Worker " + workerId
                                + " is starting the leader election..."
                        );

                        startElection();

                        break;
                    }

                    Thread.sleep(2000);
                }

            } catch (Exception e) {

                System.out.println(
                        "Worker " + workerId
                        + " could not start the election."
                );

                e.printStackTrace();
            }

        }).start();
    }

    /**
     * Main method used to start a Worker process.
     *
     * @param args command-line arguments
     */
    
        /**
     * Returns the ID of the current coordinator.
     *
     * @return coordinator worker ID
     */
    @Override
    public int getCoordinatorId() throws RemoteException {
        return coordinatorId;
    }
     
    /**
    * Checks whether the coordinator has reached the
    * maximum number of jobs for the current term.
    *
    * A new leader election starts after the fifth
    * completed client job.
    */
    private void checkJobLimitAndStartElection()
            throws RemoteException {

        if (jobsAssigned >= MAX_JOBS_PER_TERM) {

            System.out.println(
                    "Worker " + workerId
                    + " has reached the maximum of "
                    + MAX_JOBS_PER_TERM
                    + " jobs in this term."
            );

            // Reset the job counter for the new term.
            jobsAssigned = 0;

            // The current coordinator is no longer
            // considered active while the election runs.
            coordinatorId = -1;

            System.out.println(
                    "Worker " + workerId
                    + " is starting a new leader election."
            );

            startElection();
        }
    }
    /**
     * Receives a MAX job from a client.
     *
     * The coordinator divides the numbers among
     * the currently active workers.
     * numbers to process
     * @return maximum value
     */
    @Override
    public int submitMaxJob(int[] numbers)
            throws RemoteException {

        // Only the coordinator can accept client jobs.
        if (coordinatorId != workerId) {
            throw new RemoteException(
                    "Worker " + workerId
                    + " is not the coordinator."
            );
        }

        // Count this client request as one job in the current term.
        jobsAssigned++;

        System.out.println(
                "Worker " + workerId
                + " has assigned "
                + jobsAssigned
                + " job(s) in this term."
        );

        if (numbers == null || numbers.length == 0) {
            throw new RemoteException(
                    "Number list cannot be empty."
            );
        }

        // Connect to Bootstrap Node.
        Registry registry = LocateRegistry.getRegistry("localhost",1099);

        try {

            BootstrapInterface bootstrap = (BootstrapInterface) registry.lookup("BootstrapNode");

            WorkerInterface[] activeWorkers = bootstrap.getActiveWorkers();

            if (activeWorkers.length == 0) {
                throw new RemoteException(
                        "No active workers available."
                );
            }

            int workerCount = activeWorkers.length;

            int baseSize = numbers.length / workerCount;

            int remainder = numbers.length % workerCount;

            int[] results = new int[workerCount];

            Thread[] threads = new Thread[workerCount];

            int currentStart = 0;

            for (int i = 0; i < workerCount; i++) {

                int currentSize = baseSize
                        + (i < remainder ? 1 : 0);

                int startIndex = currentStart;
                int endIndex = currentStart + currentSize;

                WorkerInterface target = activeWorkers[i];

                int targetId = target.getWorkerId();

                // JAC increases when work is assigned
                // to another worker.
                if (targetId != workerId) {
                    incrementJac();
                }

                final int index = i;

                threads[i] = new Thread(() -> {

                    try {

                        int[] partition = java.util.Arrays.copyOfRange(
                                        numbers,
                                        startIndex,
                                        endIndex
                                );

                        results[index] = target.processMaxJob(partition);

                    } catch (RemoteException e) {

                        System.out.println(
                                "Could not process MAX on Worker "
                                + targetId
                        );
                    }
                });

                threads[i].start();

                currentStart = endIndex;
            }

            // Wait for all workers.
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            int finalResult = results[0];

            for (int i = 1; i < results.length; i++) {

                if (results[i] > finalResult) {
                    finalResult = results[i];
                }
            }

            System.out.println(
                    "Coordinator Worker " + workerId
                    + " completed MAX job. Result = "
                    + finalResult
            );
            
            // Start a new election after the fifth completed job.
            checkJobLimitAndStartElection();

            return finalResult;

        } catch (Exception e) {

            throw new RemoteException(
                    "Could not submit MAX job.",
                    e
            );
        }
    }

    /**
     * Receives a PRIMESUM job from a client.
     * start starting number
     *  end ending number
     * @return total sum of prime numbers
     */
    @Override
    public int submitPrimeSumJob(int start,int end)
            throws RemoteException {

        if (coordinatorId != workerId) {
            throw new RemoteException(
                    "Worker " + workerId
                    + " is not the coordinator."
            );
        }

        // Count this client request as one job in the current term.
        jobsAssigned++;

        System.out.println(
                "Worker " + workerId
                + " has assigned "
                + jobsAssigned
                + " job(s) in this term."
        );

        if (start > end) {
            throw new RemoteException(
                    "Start value cannot be greater than end value."
            );
        }

        Registry registry = LocateRegistry.getRegistry("localhost",1099);

        try {

            BootstrapInterface bootstrap = (BootstrapInterface) registry.lookup("BootstrapNode");

            WorkerInterface[] activeWorkers = bootstrap.getActiveWorkers();

            if (activeWorkers.length == 0) {
                throw new RemoteException(
                        "No active workers available."
                );
            }

            int workerCount = activeWorkers.length;

            int totalNumbers = end - start + 1;

            int baseSize = totalNumbers / workerCount;

            int remainder = totalNumbers % workerCount;

            int[] results = new int[workerCount];

            Thread[] threads = new Thread[workerCount];

            int currentStart = start;

            for (int i = 0; i < workerCount; i++) {

                int currentSize = baseSize
                        + (i < remainder ? 1 : 0);

                int currentEnd = currentStart
                        + currentSize
                        - 1;

                WorkerInterface target = activeWorkers[i];

                int targetId = target.getWorkerId();

                if (targetId != workerId) {
                    incrementJac();
                }

                final int index = i;
                final int rangeStart = currentStart;
                final int rangeEnd = currentEnd;

                threads[i] = new Thread(() -> {

                    try {

                        results[index] =
                                target.processPrimeSumJob(rangeStart,rangeEnd);

                    } catch (RemoteException e) {

                        System.out.println(
                                "Could not process PRIMESUM on Worker "
                                + targetId
                        );
                    }
                });

                threads[i].start();

                currentStart = currentEnd + 1;
            }

            // Wait for all workers.
            for (Thread thread : threads) {

                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            int totalResult = 0;

            for (int result : results) {
                totalResult += result;
            }

            System.out.println(
                    "Coordinator Worker " + workerId
                    + " completed PRIMESUM. Result = "
                    + totalResult
            );

            // Start a new election after the fifth completed job.
            checkJobLimitAndStartElection();
            return totalResult;

        } catch (Exception e) {

            throw new RemoteException(
                    "Could not submit PRIMESUM job.",
                    e
            );
        }
    }

    /**
     * Receives a PRIMECOUNT job from a client.
     * return number of prime numbers
     */
    @Override
    public int submitPrimeCountJob(
            int[] numbers)
            throws RemoteException {

        if (coordinatorId != workerId) {
            throw new RemoteException(
                    "Worker " + workerId
                    + " is not the coordinator."
            );
        }

        // Count this client request as one job in the current term.
        jobsAssigned++;

        System.out.println(
                "Worker " + workerId
                + " has assigned "
                + jobsAssigned
                + " job(s) in this term."
        );

        if (numbers == null || numbers.length == 0) {
            throw new RemoteException(
                    "Number list cannot be empty."
            );
        }

        Registry registry = LocateRegistry.getRegistry("localhost",1099);

        try {

            BootstrapInterface bootstrap = (BootstrapInterface) registry.lookup("BootstrapNode");

            WorkerInterface[] activeWorkers = bootstrap.getActiveWorkers();

            if (activeWorkers.length == 0) {
                throw new RemoteException(
                        "No active workers available."
                );
            }

            int workerCount = activeWorkers.length;

            int baseSize = numbers.length / workerCount;

            int remainder = numbers.length % workerCount;

            int[] results = new int[workerCount];

            Thread[] threads = new Thread[workerCount];

            int currentStart = 0;

            for (int i = 0; i < workerCount; i++) {

                int currentSize = baseSize
                        + (i < remainder ? 1 : 0);

                int startIndex = currentStart;

                int endIndex = currentStart + currentSize;

                WorkerInterface target = activeWorkers[i];

                int targetId = target.getWorkerId();

                if (targetId != workerId) {
                    incrementJac();
                }

                final int index = i;

                threads[i] = new Thread(() -> {

                    try {

                        int[] partition =java.util.Arrays.copyOfRange(
                                        numbers,
                                        startIndex,
                                        endIndex
                                );

                        results[index] =
                                target.processPrimeCountJob(partition);

                    } catch (RemoteException e) {

                        System.out.println(
                                "Could not process PRIMECOUNT on Worker "
                                + targetId
                        );
                    }
                });

                threads[i].start();

                currentStart = endIndex;
            }

            // Wait for all workers.
            for (Thread thread : threads) {

                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            int totalResult = 0;

            for (int result : results) {
                totalResult += result;
            }

            System.out.println(
                    "Coordinator Worker " + workerId
                    + " completed PRIMECOUNT. Result = "
                    + totalResult
            );

            // Start a new election after the fifth completed job.
            checkJobLimitAndStartElection();

            return totalResult;

        } catch (Exception e) {

            throw new RemoteException(
                    "Could not submit PRIMECOUNT job.",
                    e
            );
        }
    }
    
    public static void main(String[] args) {

        try {

            // Check that the worker ID was provided.
            if (args.length != 1) {

                System.out.println(
                        "Usage: Worker <workerID>"
                );

                return;
            }

            // Convert the argument to an integer.
            int workerId =Integer.parseInt(args[0]);

            // Create the Worker object.
            Worker worker = new Worker(workerId);

            // Connect to the existing RMI registry.
            Registry registry = LocateRegistry.getRegistry("localhost",1099);

            // Register this Worker in the RMI registry.
            registry.rebind(
                    "Worker" + workerId,
                    worker
            );

            System.out.println(
                    "Worker " + workerId
                    + " registered in the RMI registry."
            );

            System.out.println(
                    "Worker " + worker.getWorkerId()
                    + " has started."
            );

            // Connect to the Bootstrap Node.
            System.out.println(
                    "Worker " + workerId
                    + " connecting to Bootstrap Node..."
            );

            BootstrapInterface bootstrap = (BootstrapInterface)
                    registry.lookup("BootstrapNode");

            // Register this Worker with Bootstrap.
            bootstrap.registerWorker(workerId,worker);

            System.out.println(
                    "Worker " + workerId
                    + " registered with the Bootstrap Node."
            );

            // Get the currently active workers.
            WorkerInterface[] activeWorkers = bootstrap.getActiveWorkers();

            System.out.println(
                    "Worker " + workerId
                    + " found "
                    + activeWorkers.length
                    + " active worker(s) through Bootstrap Node."
            );

            for (WorkerInterface activeWorker : activeWorkers) {

                System.out.println(
                        "Active Worker ID: "
                        + activeWorker.getWorkerId()
                );
            }

            // Randomly connect to one active worker.
            connectToRandomWorker(worker,bootstrap);

            System.out.println(
                    "Worker " + workerId
                    + " is ready."
            );

            // Only Worker 1 waits for all 3 workers
            // and then starts the election.
            if (workerId == 1) {

                worker.waitForWorkersAndStartElection();
            }
        } 
        
            catch (NumberFormatException e) {

            System.out.println(
                    "Worker ID must be an integer."
            );

        } catch (NotBoundException e) {

            System.out.println(
                    "Bootstrap Node is not registered "
                    + "in the RMI registry."
            );

            e.printStackTrace();

        } catch (RemoteException e) {

            System.out.println(
                    "Could not start the Worker."
            );

            e.printStackTrace();
        }
    }
}