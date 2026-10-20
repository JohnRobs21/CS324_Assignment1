/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

package Coordinator;

import com.cs324.distrilab.grpc.JobServiceGrpc;
import com.cs324.distrilab.grpc.JobUpdate;
import com.cs324.distrilab.grpc.MaxJobRequest;
import com.cs324.distrilab.grpc.WorkerProgress;
import com.cs324.distrilab.grpc.PrimeSumJobRequest;
import com.cs324.distrilab.grpc.PrimeCountJobRequest;
import com.cs324.distrilab.grpc.TaskChunkRequest;
import com.cs324.distrilab.grpc.TaskChunkResponse;
import com.cs324.distrilab.grpc.TaskProgressUpdate;
import com.cs324.distrilab.grpc.WorkerServiceGrpc;
import com.cs324.distrilab.grpc.BootstrapServiceGrpc;
import com.cs324.distrilab.grpc.GetActiveWorkersRequest;
import com.cs324.distrilab.grpc.GetActiveWorkersResponse;
import com.cs324.distrilab.grpc.WorkerInfo;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.TimeUnit;

public class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
   
   

private java.util.List<WorkerInfo> getRegisteredWorkers() {

    ManagedChannel bootstrapChannel = ManagedChannelBuilder
            .forAddress("localhost", 50050)
            .usePlaintext()
            .build();

    try {
        var bootstrapStub =
                BootstrapServiceGrpc.newBlockingStub(bootstrapChannel)
                        .withDeadlineAfter(
                                3, java.util.concurrent.TimeUnit.SECONDS);

        // Discover workers that Bootstrap currently considers active.
        GetActiveWorkersResponse workersResponse =
                bootstrapStub.getActiveWorkers(
                        GetActiveWorkersRequest.newBuilder().build());

        java.util.List<WorkerInfo> activeWorkers =
                new java.util.ArrayList<>();

        for (WorkerInfo worker : workersResponse.getWorkersList()) {
            if (worker.getActive()) {
                activeWorkers.add(worker);
            }
        }

        if (activeWorkers.isEmpty()) {
            throw new IllegalStateException(
                    "No active workers are registered with Bootstrap Server.");
        }

        // Retrieve the currently elected coordinator.
        var coordinatorResponse =
                bootstrapStub.getCoordinator(
                        com.cs324.distrilab.grpc.GetCoordinatorRequest
                                .newBuilder()
                                .build());

        if (coordinatorResponse.getHasCoordinator()) {
            int leaderId = coordinatorResponse.getCoordinatorId();

            // Put the elected coordinator first in the worker list.
            activeWorkers.sort((workerA, workerB) -> {
                if (workerA.getWorkerId() == leaderId) {
                    return workerB.getWorkerId() == leaderId ? 0 : -1;
                }

                if (workerB.getWorkerId() == leaderId) {
                    return 1;
                }

                return Integer.compare(
                        workerA.getWorkerId(),
                        workerB.getWorkerId());
            });

            System.out.println(
                    "Job service discovered coordinator: Worker "
                    + leaderId);
        } else {
            System.out.println(
                    "No coordinator has been elected yet. "
                    + "Using the active-worker list.");
        }

        return activeWorkers;

    } finally {
        bootstrapChannel.shutdownNow();
    }
}

private TaskChunkResponse executeWithRetry(
        TaskChunkRequest task,
        java.util.List<WorkerInfo> workers,
        int preferredWorkerId) {

    Exception lastError = null;

    // Try the worker originally assigned to this task first.
    for (WorkerInfo worker : workers) {
        if (worker.getWorkerId() == preferredWorkerId) {
            try {
                return callWorker(worker, task);
            } catch (Exception e) {
                lastError = e;
                System.out.println(
                        "Worker " + worker.getWorkerId()
                        + " failed. Trying another worker...");
            }
            break;
        }
    }

    // Retry the unfinished task on another active worker.
    for (WorkerInfo worker : workers) {
        if (worker.getWorkerId() == preferredWorkerId) {
            continue;
        }

        try {
            System.out.println(
                    "Reassigning task " + task.getTaskId()
                    + " to Worker " + worker.getWorkerId());

            return callWorker(worker, task);

        } catch (Exception e) {
            lastError = e;
            System.out.println(
                    "Worker " + worker.getWorkerId()
                    + " also failed: " + e.getMessage());
        }
    }

    throw new RuntimeException(
            "Task " + task.getTaskId()
            + " failed on all available workers.",
            lastError);
}

private TaskProgressUpdate executeWithProgressRetry(
        TaskChunkRequest task,
        java.util.List<WorkerInfo> workers,
        int preferredWorkerId,
        java.util.function.Consumer<TaskProgressUpdate> progressConsumer) {

    Exception lastError = null;

    // Try the originally assigned worker first.
    for (WorkerInfo worker : workers) {
        if (worker.getWorkerId() != preferredWorkerId) {
            continue;
        }

        try {
            return callWorkerWithProgressBlocking(
                    worker, task, progressConsumer);
        } catch (Exception e) {
            lastError = e;
            System.out.println(
                    "Worker " + worker.getWorkerId()
                    + " failed during streaming. Trying another worker...");
        }

        break;
    }

    // Retry the task on another available worker.
    for (WorkerInfo worker : workers) {
        if (worker.getWorkerId() == preferredWorkerId) {
            continue;
        }

        try {
            System.out.println(
                    "Reassigning task " + task.getTaskId()
                    + " to Worker " + worker.getWorkerId());

            return callWorkerWithProgressBlocking(
                    worker, task, progressConsumer);

        } catch (Exception e) {
            lastError = e;
            System.out.println(
                    "Worker " + worker.getWorkerId()
                    + " also failed: " + e.getMessage());
        }
    }

    throw new RuntimeException(
            "Task " + task.getTaskId()
            + " failed on all available workers.",
            lastError);
}

private TaskChunkResponse callWorker(
        WorkerInfo worker,
        TaskChunkRequest task) {

    ManagedChannel workerChannel = ManagedChannelBuilder
            .forAddress(worker.getHost(), worker.getPort())
            .usePlaintext()
            .build();

    try {
        return WorkerServiceGrpc.newBlockingStub(workerChannel)
                .withDeadlineAfter(
                        120, java.util.concurrent.TimeUnit.SECONDS)
                .executeTask(task);

    } finally {
        workerChannel.shutdown();
    }
}


private void callWorkerWithProgress(
        WorkerInfo worker,
        TaskChunkRequest task,
        java.util.function.Consumer<TaskProgressUpdate> progressConsumer,
        java.util.function.Consumer<Throwable> errorConsumer) {

    ManagedChannel workerChannel = ManagedChannelBuilder
            .forAddress(worker.getHost(), worker.getPort())
            .usePlaintext()
            .build();

    try {
        WorkerServiceGrpc.newStub(workerChannel)
                .withDeadlineAfter(120, TimeUnit.SECONDS)
                .executeTaskWithProgress(
                        task,
                        new StreamObserver<TaskProgressUpdate>() {
                            @Override
                            public void onNext(TaskProgressUpdate update) {
                                progressConsumer.accept(update);
                            }

                            @Override
                            public void onError(Throwable error) {
                                try {
                                    errorConsumer.accept(error);
                                } finally {
                                    workerChannel.shutdown();
                                }
                            }

                            @Override
                            public void onCompleted() {
                                workerChannel.shutdown();
                            }
                        });

    } catch (Exception e) {
        workerChannel.shutdownNow();
        errorConsumer.accept(e);
    }
}


private TaskProgressUpdate callWorkerWithProgressBlocking(
        WorkerInfo worker,
        TaskChunkRequest task,
        java.util.function.Consumer<TaskProgressUpdate> progressConsumer) {

    ManagedChannel workerChannel = ManagedChannelBuilder
            .forAddress(worker.getHost(), worker.getPort())
            .usePlaintext()
            .build();

    try {
        java.util.Iterator<TaskProgressUpdate> updates =
                WorkerServiceGrpc.newBlockingStub(workerChannel)
                        .withDeadlineAfter(120, TimeUnit.SECONDS)
                        .executeTaskWithProgress(task);

        TaskProgressUpdate finalUpdate = null;

        // Receive progress updates as the worker calculates.
        while (updates.hasNext()) {
            TaskProgressUpdate update = updates.next();

            // Forward each update to the coordinator's progress handler.
            progressConsumer.accept(update);

            // Keep the final update for the actual result.
            if (update.getCompleted()) {
                finalUpdate = update;
            }
        }

        if (finalUpdate == null) {
            throw new RuntimeException(
                    "Worker " + worker.getWorkerId()
                    + " ended without a final result.");
        }

        if (!finalUpdate.getSuccess()) {
            throw new RuntimeException(finalUpdate.getMessage());
        }

        return finalUpdate;

    } finally {
        workerChannel.shutdown();
    }
}
  
@Override
public void submitMax(
        MaxJobRequest request,
        StreamObserver<JobUpdate> responseObserver) {

    java.util.concurrent.ExecutorService executor =
            java.util.concurrent.Executors.newFixedThreadPool(2);

    try {
        java.util.List<Integer> numbers = request.getNumbersList();

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "MAX requires at least one number.");
        }

        java.util.List<WorkerInfo> workers = getRegisteredWorkers();

        if (workers.isEmpty()) {
            throw new IllegalStateException(
                    "No active workers are available.");
        }

        int middle = (numbers.size() + 1) / 2;

        java.util.List<Integer> firstHalf =
                new java.util.ArrayList<>(
                        numbers.subList(0, middle));

        java.util.List<Integer> secondHalf =
                new java.util.ArrayList<>(
                        numbers.subList(middle, numbers.size()));

        WorkerInfo worker1 = workers.get(0);
        WorkerInfo worker2 = workers.size() > 1
                ? workers.get(1) : null;

        if (worker2 == null) {
            firstHalf = new java.util.ArrayList<>(numbers);
            secondHalf = new java.util.ArrayList<>();
        }

        final int task1Total = firstHalf.size();
        final int task2Total = secondHalf.size();
        final int totalUnits = numbers.size();

        final Object progressLock = new Object();

        final java.util.concurrent.atomic.AtomicInteger completed1 =
                new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicInteger completed2 =
                new java.util.concurrent.atomic.AtomicInteger(0);

        final java.util.concurrent.atomic.AtomicInteger progressWorker1 =
                new java.util.concurrent.atomic.AtomicInteger(
                        worker1.getWorkerId());
        final java.util.concurrent.atomic.AtomicInteger progressWorker2 =
                new java.util.concurrent.atomic.AtomicInteger(
                        worker2 == null ? -1 : worker2.getWorkerId());

        final java.util.List<WorkerInfo> availableWorkers = workers;

        TaskChunkRequest task1 = TaskChunkRequest.newBuilder()
                .setJobId(request.getJobId())
                .setTaskId(request.getJobId() + "-task1")
                .setJobType("MAX")
                .addAllNumbers(firstHalf)
                .build();

        java.util.concurrent.Future<TaskProgressUpdate> future1 =
                executor.submit(() ->
                        executeWithProgressRetry(
                                task1,
                                availableWorkers,
                                worker1.getWorkerId(),
                                progress -> {
                                    synchronized (progressLock) {
                                        completed1.set(Math.min(
                                                task1Total,
                                                progress.getCompletedUnits()));

                                        if (progress.getWorkerId() > 0) {
                                            progressWorker1.set(
                                                    progress.getWorkerId());
                                        }

                                        JobUpdate.Builder update =
                                                JobUpdate.newBuilder()
                                                        .setJobId(request.getJobId())
                                                        .setOverallPercentage(
                                                                (completed1.get()
                                                                + completed2.get())
                                                                * 100.0
                                                                / totalUnits)
                                                        .setCompleted(false)
                                                        .setMessage(
                                                                "Searching for maximum...");

                                        update.addWorkerProgress(
                                                WorkerProgress.newBuilder()
                                                        .setWorkerId(
                                                                progressWorker1.get())
                                                        .setCompletedUnits(
                                                                completed1.get())
                                                        .setTotalUnits(task1Total)
                                                        .setPercentage(
                                                                completed1.get()
                                                                * 100.0
                                                                / task1Total)
                                                        .setStatus(
                                                                completed1.get()
                                                                >= task1Total
                                                                ? "Completed"
                                                                : "Processing")
                                                        .build());

                                        if (task2Total > 0) {
                                            update.addWorkerProgress(
                                                    WorkerProgress.newBuilder()
                                                            .setWorkerId(
                                                                    progressWorker2.get())
                                                            .setCompletedUnits(
                                                                    completed2.get())
                                                            .setTotalUnits(task2Total)
                                                            .setPercentage(
                                                                    completed2.get()
                                                                    * 100.0
                                                                    / task2Total)
                                                            .setStatus(
                                                                    completed2.get()
                                                                    >= task2Total
                                                                    ? "Completed"
                                                                    : "Processing")
                                                            .build());
                                        }

                                        responseObserver.onNext(update.build());
                                    }
                                }));

        java.util.concurrent.Future<TaskProgressUpdate> future2 = null;

        if (task2Total > 0 && worker2 != null) {
            TaskChunkRequest task2 = TaskChunkRequest.newBuilder()
                    .setJobId(request.getJobId())
                    .setTaskId(request.getJobId() + "-task2")
                    .setJobType("MAX")
                    .addAllNumbers(secondHalf)
                    .build();

            future2 = executor.submit(() ->
                    executeWithProgressRetry(
                            task2,
                            availableWorkers,
                            worker2.getWorkerId(),
                            progress -> {
                                synchronized (progressLock) {
                                    completed2.set(Math.min(
                                            task2Total,
                                            progress.getCompletedUnits()));

                                    if (progress.getWorkerId() > 0) {
                                        progressWorker2.set(
                                                progress.getWorkerId());
                                    }

                                    JobUpdate.Builder update =
                                            JobUpdate.newBuilder()
                                                    .setJobId(request.getJobId())
                                                    .setOverallPercentage(
                                                            (completed1.get()
                                                            + completed2.get())
                                                            * 100.0
                                                            / totalUnits)
                                                    .setCompleted(false)
                                                    .setMessage(
                                                            "Searching for maximum...");

                                    update.addWorkerProgress(
                                            WorkerProgress.newBuilder()
                                                    .setWorkerId(
                                                            progressWorker1.get())
                                                    .setCompletedUnits(
                                                            completed1.get())
                                                    .setTotalUnits(task1Total)
                                                    .setPercentage(
                                                            completed1.get()
                                                            * 100.0
                                                            / task1Total)
                                                    .setStatus(
                                                            completed1.get()
                                                            >= task1Total
                                                            ? "Completed"
                                                            : "Processing")
                                                    .build());

                                    update.addWorkerProgress(
                                            WorkerProgress.newBuilder()
                                                    .setWorkerId(
                                                            progressWorker2.get())
                                                    .setCompletedUnits(
                                                            completed2.get())
                                                    .setTotalUnits(task2Total)
                                                    .setPercentage(
                                                            completed2.get()
                                                            * 100.0
                                                            / task2Total)
                                                    .setStatus(
                                                            completed2.get()
                                                            >= task2Total
                                                            ? "Completed"
                                                            : "Processing")
                                                    .build());

                                    responseObserver.onNext(update.build());
                                }
                            }));
        }

        // Collect the final result from each worker.
        TaskProgressUpdate result1 = future1.get();

        if (!result1.getSuccess()) {
            throw new RuntimeException(result1.getMessage());
        }

        long maximum = result1.getResult();

        if (future2 != null) {
            TaskProgressUpdate result2 = future2.get();

            if (!result2.getSuccess()) {
                throw new RuntimeException(result2.getMessage());
            }

            maximum = Math.max(maximum, result2.getResult());
        }

        // Send the final result and completed progress.
        synchronized (progressLock) {
            completed1.set(task1Total);

            if (task2Total > 0) {
                completed2.set(task2Total);
            }

            JobUpdate.Builder finalUpdate = JobUpdate.newBuilder()
                    .setJobId(request.getJobId())
                    .setOverallPercentage(100.0)
                    .setCompleted(true)
                    .setResult(maximum)
                    .setMessage("MAX completed using streaming workers");

            finalUpdate.addWorkerProgress(
                    WorkerProgress.newBuilder()
                            .setWorkerId(progressWorker1.get())
                            .setCompletedUnits(task1Total)
                            .setTotalUnits(task1Total)
                            .setPercentage(100.0)
                            .setStatus("Completed")
                            .build());

            if (task2Total > 0) {
                finalUpdate.addWorkerProgress(
                        WorkerProgress.newBuilder()
                                .setWorkerId(progressWorker2.get())
                                .setCompletedUnits(task2Total)
                                .setTotalUnits(task2Total)
                                .setPercentage(100.0)
                                .setStatus("Completed")
                                .build());
            }

            responseObserver.onNext(finalUpdate.build());
        }

        responseObserver.onCompleted();

    } catch (Exception e) {
        responseObserver.onError(
                io.grpc.Status.INTERNAL
                        .withDescription("MAX job failed: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException());

    } finally {
        executor.shutdown();
    }
}

@Override
public void submitPrimeSum(
        PrimeSumJobRequest request,
        StreamObserver<JobUpdate> responseObserver) {

    java.util.concurrent.ExecutorService executor =
            java.util.concurrent.Executors.newFixedThreadPool(2);

    try {
        int start = request.getStart();
        int end = request.getEnd();

        if (start > end) {
            throw new IllegalArgumentException(
                    "Start must be less than or equal to end.");
        }

        java.util.List<WorkerInfo> workers = getRegisteredWorkers();

        if (workers.isEmpty()) {
            throw new IllegalStateException(
                    "No active workers are available.");
        }

        int middle = start + (end - start) / 2;

        WorkerInfo worker1 = workers.get(0);
        WorkerInfo worker2 = workers.size() > 1
                ? workers.get(1) : null;

        if (worker2 == null) {
            middle = end;
        }

        final int task1Total = middle - start + 1;
        final int task2Total = worker2 == null
                ? 0 : end - middle;
        final int totalUnits = task1Total + task2Total;

        final Object progressLock = new Object();

        final java.util.concurrent.atomic.AtomicInteger completed1 =
                new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicInteger completed2 =
                new java.util.concurrent.atomic.AtomicInteger(0);

        final java.util.concurrent.atomic.AtomicInteger progressWorker1 =
                new java.util.concurrent.atomic.AtomicInteger(
                        worker1.getWorkerId());
        final java.util.concurrent.atomic.AtomicInteger progressWorker2 =
                new java.util.concurrent.atomic.AtomicInteger(
                        worker2 == null ? -1 : worker2.getWorkerId());

        TaskChunkRequest task1 = TaskChunkRequest.newBuilder()
                .setJobId(request.getJobId())
                .setTaskId(request.getJobId() + "-task1")
                .setJobType("PRIMESUM")
                .setStart(start)
                .setEnd(middle)
                .build();

        final java.util.List<WorkerInfo> availableWorkers = workers;

        java.util.concurrent.Future<TaskProgressUpdate> future1 =
                executor.submit(() ->
                        executeWithProgressRetry(
                                task1,
                                availableWorkers,
                                worker1.getWorkerId(),
                                progress -> {
                                    synchronized (progressLock) {
                                        completed1.set(Math.min(
                                                task1Total,
                                                progress.getCompletedUnits()));

                                        if (progress.getWorkerId() > 0) {
                                            progressWorker1.set(
                                                    progress.getWorkerId());
                                        }

                                        JobUpdate.Builder update =
                                                JobUpdate.newBuilder()
                                                        .setJobId(request.getJobId())
                                                        .setOverallPercentage(
                                                                (completed1.get()
                                                                + completed2.get())
                                                                * 100.0
                                                                / totalUnits)
                                                        .setCompleted(false)
                                                        .setMessage(
                                                                "Calculating prime sum...");

                                        update.addWorkerProgress(
                                                WorkerProgress.newBuilder()
                                                        .setWorkerId(
                                                                progressWorker1.get())
                                                        .setCompletedUnits(
                                                                completed1.get())
                                                        .setTotalUnits(task1Total)
                                                        .setPercentage(
                                                                completed1.get()
                                                                * 100.0
                                                                / task1Total)
                                                        .setStatus(
                                                                completed1.get()
                                                                >= task1Total
                                                                ? "Completed"
                                                                : "Processing")
                                                        .build());

                                        if (worker2 != null) {
                                            update.addWorkerProgress(
                                                    WorkerProgress.newBuilder()
                                                            .setWorkerId(
                                                                    progressWorker2.get())
                                                            .setCompletedUnits(
                                                                    completed2.get())
                                                            .setTotalUnits(task2Total)
                                                            .setPercentage(
                                                                    completed2.get()
                                                                    * 100.0
                                                                    / task2Total)
                                                            .setStatus(
                                                                    completed2.get()
                                                                    >= task2Total
                                                                    ? "Completed"
                                                                    : "Processing")
                                                            .build());
                                        }

                                        responseObserver.onNext(update.build());
                                    }
                                }));

        java.util.concurrent.Future<TaskProgressUpdate> future2 = null;

        if (worker2 != null && middle < end) {
            TaskChunkRequest task2 = TaskChunkRequest.newBuilder()
                    .setJobId(request.getJobId())
                    .setTaskId(request.getJobId() + "-task2")
                    .setJobType("PRIMESUM")
                    .setStart(middle + 1)
                    .setEnd(end)
                    .build();

            future2 = executor.submit(() ->
                    executeWithProgressRetry(
                            task2,
                            availableWorkers,
                            worker2.getWorkerId(),
                            progress -> {
                                synchronized (progressLock) {
                                    completed2.set(Math.min(
                                            task2Total,
                                            progress.getCompletedUnits()));

                                    if (progress.getWorkerId() > 0) {
                                        progressWorker2.set(
                                                progress.getWorkerId());
                                    }

                                    JobUpdate.Builder update =
                                            JobUpdate.newBuilder()
                                                    .setJobId(request.getJobId())
                                                    .setOverallPercentage(
                                                            (completed1.get()
                                                            + completed2.get())
                                                            * 100.0
                                                            / totalUnits)
                                                    .setCompleted(false)
                                                    .setMessage(
                                                            "Calculating prime sum...");

                                    update.addWorkerProgress(
                                            WorkerProgress.newBuilder()
                                                    .setWorkerId(
                                                            progressWorker1.get())
                                                    .setCompletedUnits(
                                                            completed1.get())
                                                    .setTotalUnits(task1Total)
                                                    .setPercentage(
                                                            completed1.get()
                                                            * 100.0
                                                            / task1Total)
                                                    .setStatus(
                                                            completed1.get()
                                                            >= task1Total
                                                            ? "Completed"
                                                            : "Processing")
                                                    .build());

                                    update.addWorkerProgress(
                                            WorkerProgress.newBuilder()
                                                    .setWorkerId(
                                                            progressWorker2.get())
                                                    .setCompletedUnits(
                                                            completed2.get())
                                                    .setTotalUnits(task2Total)
                                                    .setPercentage(
                                                            completed2.get()
                                                            * 100.0
                                                            / task2Total)
                                                    .setStatus(
                                                            completed2.get()
                                                            >= task2Total
                                                            ? "Completed"
                                                            : "Processing")
                                                    .build());

                                    responseObserver.onNext(update.build());
                                }
                            }));
        }

        // Wait for the final result from each streaming task.
        TaskProgressUpdate result1 = future1.get();

        if (!result1.getSuccess()) {
            throw new RuntimeException(result1.getMessage());
        }

        long totalSum = result1.getResult();

        if (future2 != null) {
            TaskProgressUpdate result2 = future2.get();

            if (!result2.getSuccess()) {
                throw new RuntimeException(result2.getMessage());
            }

            totalSum += result2.getResult();
        }

        // Send the final completed update.
        synchronized (progressLock) {
            completed1.set(task1Total);

            if (worker2 == null || middle >= end) {
                completed2.set(0);
            } else {
                completed2.set(task2Total);
            }

            JobUpdate.Builder finalUpdate = JobUpdate.newBuilder()
                    .setJobId(request.getJobId())
                    .setOverallPercentage(100.0)
                    .setCompleted(true)
                    .setResult(totalSum)
                    .setMessage(
                            "PRIMESUM completed using streaming workers");

            finalUpdate.addWorkerProgress(
                    WorkerProgress.newBuilder()
                            .setWorkerId(progressWorker1.get())
                            .setCompletedUnits(task1Total)
                            .setTotalUnits(task1Total)
                            .setPercentage(100.0)
                            .setStatus("Completed")
                            .build());

            if (worker2 != null && middle < end) {
                finalUpdate.addWorkerProgress(
                        WorkerProgress.newBuilder()
                                .setWorkerId(progressWorker2.get())
                                .setCompletedUnits(task2Total)
                                .setTotalUnits(task2Total)
                                .setPercentage(100.0)
                                .setStatus("Completed")
                                .build());
            }

            responseObserver.onNext(finalUpdate.build());
        }

        responseObserver.onCompleted();

    } catch (Exception e) {
        responseObserver.onError(
                io.grpc.Status.INTERNAL
                        .withDescription(
                                "PRIMESUM job failed: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException());

    } finally {
        executor.shutdown();
    }
}


@Override
public void submitPrimeCount(
        PrimeCountJobRequest request,
        StreamObserver<JobUpdate> responseObserver) {

    java.util.concurrent.ExecutorService executor =
            java.util.concurrent.Executors.newFixedThreadPool(2);

    try {
        java.util.List<Integer> numbers = request.getNumbersList();

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "PRIMECOUNT requires at least one number.");
        }

        java.util.List<WorkerInfo> workers = getRegisteredWorkers();

        if (workers.isEmpty()) {
            throw new IllegalStateException(
                    "No active workers are available.");
        }

        int middle = (numbers.size() + 1) / 2;

        java.util.List<Integer> firstHalf =
                new java.util.ArrayList<>(
                        numbers.subList(0, middle));

        java.util.List<Integer> secondHalf =
                new java.util.ArrayList<>(
                        numbers.subList(middle, numbers.size()));

        WorkerInfo worker1 = workers.get(0);
        WorkerInfo worker2 = workers.size() > 1
                ? workers.get(1) : null;

        if (worker2 == null) {
            firstHalf = new java.util.ArrayList<>(numbers);
            secondHalf = new java.util.ArrayList<>();
        }

        final int task1Total = firstHalf.size();
        final int task2Total = secondHalf.size();
        final int totalUnits = numbers.size();

        final Object progressLock = new Object();

        final java.util.concurrent.atomic.AtomicInteger completed1 =
                new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicInteger completed2 =
                new java.util.concurrent.atomic.AtomicInteger(0);

        final java.util.concurrent.atomic.AtomicInteger progressWorker1 =
                new java.util.concurrent.atomic.AtomicInteger(
                        worker1.getWorkerId());
        final java.util.concurrent.atomic.AtomicInteger progressWorker2 =
                new java.util.concurrent.atomic.AtomicInteger(
                        worker2 == null ? -1 : worker2.getWorkerId());

        final java.util.List<WorkerInfo> availableWorkers = workers;

        TaskChunkRequest task1 = TaskChunkRequest.newBuilder()
                .setJobId(request.getJobId())
                .setTaskId(request.getJobId() + "-task1")
                .setJobType("PRIMECOUNT")
                .addAllNumbers(firstHalf)
                .build();

        java.util.concurrent.Future<TaskProgressUpdate> future1 =
                executor.submit(() ->
                        executeWithProgressRetry(
                                task1,
                                availableWorkers,
                                worker1.getWorkerId(),
                                progress -> {
                                    synchronized (progressLock) {
                                        completed1.set(Math.min(
                                                task1Total,
                                                progress.getCompletedUnits()));

                                        if (progress.getWorkerId() > 0) {
                                            progressWorker1.set(
                                                    progress.getWorkerId());
                                        }

                                        JobUpdate.Builder update =
                                                JobUpdate.newBuilder()
                                                        .setJobId(request.getJobId())
                                                        .setOverallPercentage(
                                                                (completed1.get()
                                                                + completed2.get())
                                                                * 100.0
                                                                / totalUnits)
                                                        .setCompleted(false)
                                                        .setMessage(
                                                                "Counting prime numbers...");

                                        update.addWorkerProgress(
                                                WorkerProgress.newBuilder()
                                                        .setWorkerId(
                                                                progressWorker1.get())
                                                        .setCompletedUnits(
                                                                completed1.get())
                                                        .setTotalUnits(task1Total)
                                                        .setPercentage(
                                                                completed1.get()
                                                                * 100.0
                                                                / task1Total)
                                                        .setStatus(
                                                                completed1.get()
                                                                >= task1Total
                                                                ? "Completed"
                                                                : "Processing")
                                                        .build());

                                        if (task2Total > 0) {
                                            update.addWorkerProgress(
                                                    WorkerProgress.newBuilder()
                                                            .setWorkerId(
                                                                    progressWorker2.get())
                                                            .setCompletedUnits(
                                                                    completed2.get())
                                                            .setTotalUnits(task2Total)
                                                            .setPercentage(
                                                                    completed2.get()
                                                                    * 100.0
                                                                    / task2Total)
                                                            .setStatus(
                                                                    completed2.get()
                                                                    >= task2Total
                                                                    ? "Completed"
                                                                    : "Processing")
                                                            .build());
                                        }

                                        responseObserver.onNext(update.build());
                                    }
                                }));

        java.util.concurrent.Future<TaskProgressUpdate> future2 = null;

        if (task2Total > 0 && worker2 != null) {
            TaskChunkRequest task2 = TaskChunkRequest.newBuilder()
                    .setJobId(request.getJobId())
                    .setTaskId(request.getJobId() + "-task2")
                    .setJobType("PRIMECOUNT")
                    .addAllNumbers(secondHalf)
                    .build();

            future2 = executor.submit(() ->
                    executeWithProgressRetry(
                            task2,
                            availableWorkers,
                            worker2.getWorkerId(),
                            progress -> {
                                synchronized (progressLock) {
                                    completed2.set(Math.min(
                                            task2Total,
                                            progress.getCompletedUnits()));

                                    if (progress.getWorkerId() > 0) {
                                        progressWorker2.set(
                                                progress.getWorkerId());
                                    }

                                    JobUpdate.Builder update =
                                            JobUpdate.newBuilder()
                                                    .setJobId(request.getJobId())
                                                    .setOverallPercentage(
                                                            (completed1.get()
                                                            + completed2.get())
                                                            * 100.0
                                                            / totalUnits)
                                                    .setCompleted(false)
                                                    .setMessage(
                                                            "Counting prime numbers...");

                                    update.addWorkerProgress(
                                            WorkerProgress.newBuilder()
                                                    .setWorkerId(
                                                            progressWorker1.get())
                                                    .setCompletedUnits(
                                                            completed1.get())
                                                    .setTotalUnits(task1Total)
                                                    .setPercentage(
                                                            completed1.get()
                                                            * 100.0
                                                            / task1Total)
                                                    .setStatus(
                                                            completed1.get()
                                                            >= task1Total
                                                            ? "Completed"
                                                            : "Processing")
                                                    .build());

                                    update.addWorkerProgress(
                                            WorkerProgress.newBuilder()
                                                    .setWorkerId(
                                                            progressWorker2.get())
                                                    .setCompletedUnits(
                                                            completed2.get())
                                                    .setTotalUnits(task2Total)
                                                    .setPercentage(
                                                            completed2.get()
                                                            * 100.0
                                                            / task2Total)
                                                    .setStatus(
                                                            completed2.get()
                                                            >= task2Total
                                                            ? "Completed"
                                                            : "Processing")
                                                    .build());

                                    responseObserver.onNext(update.build());
                                }
                            }));
        }

        // Collect each worker's final result.
        TaskProgressUpdate result1 = future1.get();

        if (!result1.getSuccess()) {
            throw new RuntimeException(result1.getMessage());
        }

        long totalCount = result1.getResult();

        if (future2 != null) {
            TaskProgressUpdate result2 = future2.get();

            if (!result2.getSuccess()) {
                throw new RuntimeException(result2.getMessage());
            }

            totalCount += result2.getResult();
        }

        // Send final progress and the combined count.
        synchronized (progressLock) {
            completed1.set(task1Total);

            if (task2Total > 0) {
                completed2.set(task2Total);
            }

            JobUpdate.Builder finalUpdate = JobUpdate.newBuilder()
                    .setJobId(request.getJobId())
                    .setOverallPercentage(100.0)
                    .setCompleted(true)
                    .setResult(totalCount)
                    .setMessage(
                            "PRIMECOUNT completed using streaming workers");

            finalUpdate.addWorkerProgress(
                    WorkerProgress.newBuilder()
                            .setWorkerId(progressWorker1.get())
                            .setCompletedUnits(task1Total)
                            .setTotalUnits(task1Total)
                            .setPercentage(100.0)
                            .setStatus("Completed")
                            .build());

            if (task2Total > 0) {
                finalUpdate.addWorkerProgress(
                        WorkerProgress.newBuilder()
                                .setWorkerId(progressWorker2.get())
                                .setCompletedUnits(task2Total)
                                .setTotalUnits(task2Total)
                                .setPercentage(100.0)
                                .setStatus("Completed")
                                .build());
            }

            responseObserver.onNext(finalUpdate.build());
        }

        responseObserver.onCompleted();

    } catch (Exception e) {
        responseObserver.onError(
                io.grpc.Status.INTERNAL
                        .withDescription(
                                "PRIMECOUNT job failed: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException());

    } finally {
        executor.shutdown();
    }
  }
}
