/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */


package worker;

import com.cs324.distrilab.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WorkerImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final int workerId;
    private final int jac;

    private volatile int coordinatorId = -1;
    private volatile int coordinatorJac = -1;
    private volatile String currentElectionId = "";

    private final ExecutorService executor =
            Executors.newFixedThreadPool(
                    Math.max(2, Runtime.getRuntime().availableProcessors()));

    public WorkerImpl(int workerId) {
        this.workerId = workerId;
        this.jac = workerId;
    }

    @Override
    public void startElection(
            ElectionRequest request,
            StreamObserver<CoordinatorAnnouncement> observer) {

        try {
            // Get the currently active workers from Bootstrap.
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", 50050)
                    .usePlaintext()
                    .build();

            GetActiveWorkersResponse response;
            try {
                response = BootstrapServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(3, TimeUnit.SECONDS)
                        .getActiveWorkers(GetActiveWorkersRequest.newBuilder()
                                .build());
            } finally {
                channel.shutdown();
            }

            WorkerInfo winner = null;

            for (WorkerInfo worker : response.getWorkersList()) {
                if (!worker.getActive()) {
                    continue;
                }

                if (winner == null
                        || worker.getJac() < winner.getJac()
                        || (worker.getJac() == winner.getJac()
                        && worker.getWorkerId() > winner.getWorkerId())) {
                    winner = worker;
                }
            }

            if (winner == null) {
                throw new IllegalStateException("No active workers available.");
            }

            CoordinatorAnnouncement announcement =
                    CoordinatorAnnouncement.newBuilder()
                            .setElectionId(request.getElectionId())
                            .setCoordinatorId(winner.getWorkerId())
                            .setCoordinatorJac(winner.getJac())
                            .build();

            currentElectionId = announcement.getElectionId();
            coordinatorId = announcement.getCoordinatorId();
            coordinatorJac = announcement.getCoordinatorJac();

            // Announce the result to every active worker.
            for (WorkerInfo worker : response.getWorkersList()) {
                if (!worker.getActive()) {
                    continue;
                }

                ManagedChannel workerChannel = ManagedChannelBuilder
                        .forAddress(worker.getHost(), worker.getPort())
                        .usePlaintext()
                        .build();

                try {
                    WorkerServiceGrpc.newBlockingStub(workerChannel)
                            .withDeadlineAfter(3, TimeUnit.SECONDS)
                            .announceCoordinator(announcement);
                } catch (Exception e) {
                    System.err.println("Could not notify Worker "
                            + worker.getWorkerId() + ": " + e.getMessage());
                } finally {
                    workerChannel.shutdown();
                }
            }

            System.out.println("Election complete. Coordinator is Worker "
                    + coordinatorId + " (JAC " + coordinatorJac + ").");

            observer.onNext(announcement);
            observer.onCompleted();

        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void receiveElection(
            ElectionRequest request,
            StreamObserver<ElectionRequest> observer) {

        // Record the election ID. The initiating worker performs
        // the active-worker comparison and announces the winner.
        currentElectionId = request.getElectionId();

        observer.onNext(request);
        observer.onCompleted();
    }

    
@Override
public void announceCoordinator(
        CoordinatorAnnouncement request,
        StreamObserver<CoordinatorAnnouncement> observer) {

    // Store the elected coordinator locally.
    currentElectionId = request.getElectionId();
    coordinatorId = request.getCoordinatorId();
    coordinatorJac = request.getCoordinatorJac();

    // Notify Bootstrap so other services can discover the leader.
    ManagedChannel bootstrapChannel = ManagedChannelBuilder
            .forAddress("localhost", 50050)
            .usePlaintext()
            .build();

    try {
        CoordinatorAnnouncement recorded =
                com.cs324.distrilab.grpc.BootstrapServiceGrpc
                        .newBlockingStub(bootstrapChannel)
                        .withDeadlineAfter(3, TimeUnit.SECONDS)
                        .announceCoordinator(request);

        System.out.println("Worker " + workerId
                + " recognises Worker " + coordinatorId
                + " as coordinator.");

        observer.onNext(recorded);
        observer.onCompleted();

    } catch (Exception e) {
        observer.onError(
                io.grpc.Status.UNAVAILABLE
                        .withDescription(
                                "Could not record coordinator with Bootstrap: "
                                + e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
    } finally {
        bootstrapChannel.shutdownNow();
    }
}

   
    @Override
    public void executeTask(
            TaskChunkRequest request,
            StreamObserver<TaskChunkResponse> observer) {

        try {
            Future<Long> future =
                    executor.submit(() -> calculateResult(request));

            long result = future.get();

            int totalUnits = "PRIMESUM".equalsIgnoreCase(
                    request.getJobType())
                    ? Math.max(0,
                            request.getEnd() - request.getStart() + 1)
                    : request.getNumbersCount();

            observer.onNext(TaskChunkResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setTaskId(request.getTaskId())
                    .setWorkerId(workerId)
                    .setResult(result)
                    .setCompletedUnits(totalUnits)
                    .setTotalUnits(totalUnits)
                    .setSuccess(true)
                    .setMessage("Task completed successfully")
                    .build());

            observer.onCompleted();

        } catch (Exception e) {
            observer.onNext(TaskChunkResponse.newBuilder()
                    .setJobId(request.getJobId())
                    .setTaskId(request.getTaskId())
                    .setWorkerId(workerId)
                    .setSuccess(false)
                    .setMessage("Task failed: " + e.getMessage())
                    .build());

            observer.onCompleted();
        }
    }
    
@Override
public void executeTaskWithProgress(
        TaskChunkRequest request,
        StreamObserver<TaskProgressUpdate> observer) {

    try {
        String jobType = request.getJobType().toUpperCase();
        long result = 0L;

        int totalUnits;

        if ("PRIMESUM".equals(jobType)) {
            long rangeSize =
                    (long) request.getEnd() - request.getStart() + 1L;

            if (rangeSize <= 0 || rangeSize > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "Invalid or unsupported PRIMESUM range.");
            }

            totalUnits = (int) rangeSize;

        } else {
            totalUnits = request.getNumbersCount();
        }

        if (totalUnits <= 0) {
            throw new IllegalArgumentException(
                    "The task must contain at least one unit of work.");
        }

        int completedUnits = 0;

        // Process PRIMESUM in actual range segments.
        if ("PRIMESUM".equals(jobType)) {
            int start = request.getStart();
            int end = request.getEnd();

            int segmentSize = Math.max(10_000, totalUnits / 100);
            long nextStart = start;

            while (nextStart <= end) {
                long nextEnd = Math.min(
                        (long) end,
                        nextStart + segmentSize - 1L);

                result += sumPrimesInRange(
                        (int) nextStart, (int) nextEnd);

                completedUnits += (int) (nextEnd - nextStart + 1L);

                sendProgress(
                        request, observer, completedUnits,
                        totalUnits, result, false,
                        "Processing PRIMESUM");

                nextStart = nextEnd + 1L;
            }

        } else if ("MAX".equals(jobType)) {
            result = request.getNumbers(0);

            for (int number : request.getNumbersList()) {
                result = Math.max(result, number);
                completedUnits++;

                if (shouldReportProgress(completedUnits, totalUnits)) {
                    sendProgress(
                            request, observer, completedUnits,
                            totalUnits, result, false,
                            "Processing MAX");
                }
            }

        } else if ("PRIMECOUNT".equals(jobType)) {
            for (int number : request.getNumbersList()) {
                if (isPrime(number)) {
                    result++;
                }

                completedUnits++;

                if (shouldReportProgress(completedUnits, totalUnits)) {
                    sendProgress(
                            request, observer, completedUnits,
                            totalUnits, result, false,
                            "Processing PRIMECOUNT");
                }
            }

        } else {
            throw new IllegalArgumentException(
                    "Unsupported job type: " + jobType);
        }

        // Send the final completed update with the actual result.
        sendProgress(
                request, observer, totalUnits,
                totalUnits, result, true,
                "Task completed successfully");

        observer.onCompleted();

    } catch (Exception e) {
        observer.onError(
                io.grpc.Status.INTERNAL
                        .withDescription("Task failed: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
    }
}

private boolean shouldReportProgress(int completed, int total) {
    int interval = Math.max(1, total / 100);
    return completed % interval == 0 || completed == total;
}

private void sendProgress(
        TaskChunkRequest request,
        StreamObserver<TaskProgressUpdate> observer,
        int completedUnits,
        int totalUnits,
        long result,
        boolean completed,
        String message) {

    double percentage =
            totalUnits == 0
                    ? 100.0
                    : completedUnits * 100.0 / totalUnits;

    observer.onNext(TaskProgressUpdate.newBuilder()
            .setJobId(request.getJobId())
            .setTaskId(request.getTaskId())
            .setWorkerId(workerId)
            .setCompletedUnits(completedUnits)
            .setTotalUnits(totalUnits)
            .setPercentage(percentage)
            .setCompleted(completed)
            .setResult(result)
            .setSuccess(true)
            .setMessage(message)
            .build());
}

    private long calculateResult(TaskChunkRequest request) {
        String jobType = request.getJobType().toUpperCase();
        switch (jobType) {
            case "MAX":
                if (request.getNumbersCount() == 0) {
                    throw new IllegalArgumentException(
                            "MAX requires at least one number.");
                }

                int maximum = request.getNumbers(0);
                for (int number : request.getNumbersList()) {
                    maximum = Math.max(maximum, number);
                }
                return maximum;

            case "PRIMESUM":
            return sumPrimesInRange(request.getStart(),request.getEnd());

            case "PRIMECOUNT":
                long count = 0;
                for (int number : request.getNumbersList()) {
                    if (isPrime(number)) {
                        count++;
                    }
                }
                return count;

            default:
                throw new IllegalArgumentException(
                        "Unsupported job type: " + jobType);
        }
    }
        
        
private long sumPrimesInRange(int start, int end) {
    if (end < 2 || start > end) {
        return 0L;
    }


    int low = Math.max(start, 2);

    // Generate all primes up to sqrt(end).
    int limit = (int) Math.sqrt(end);
    boolean[] composite = new boolean[limit + 1];

    for (int i = 2; i <= limit / i; i++) {
        if (!composite[i]) {
            for (int j = i * i; j <= limit; j += i) {
                composite[j] = true;
            }
        }
    }

    java.util.List<Integer> basePrimes =
            new java.util.ArrayList<>();

    for (int i = 2; i <= limit; i++) {
        if (!composite[i]) {
            basePrimes.add(i);
        }
    }

    // Mark composite numbers in the requested range.
    int length = end - low + 1;
    boolean[] marked = new boolean[length];

    for (int prime : basePrimes) {
        long first = Math.max(
                (long) prime * prime,
                ((long) low + prime - 1) / prime * prime);

        for (long multiple = first;
             multiple <= end;
             multiple += prime) {
            marked[(int) (multiple - low)] = true;
        }
    }

    long sum = 0L;

    for (int i = 0; i < length; i++) {
        if (!marked[i]) {
            sum += (long) low + i;
        }
    }

    return sum;
}

    private boolean isPrime(int number) {
        if (number < 2) {
            return false;
        }

        for (int divisor = 2;
             divisor <= number / divisor; divisor++) {
            if (number % divisor == 0) {
                return false;
            }
        }

        return true;
    }
}
