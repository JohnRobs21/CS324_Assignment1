/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */


package Bootstrap;

import com.cs324.distrilab.grpc.ElectionRequest;
import com.cs324.distrilab.grpc.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.UUID;
import com.cs324.distrilab.grpc.BootstrapServiceGrpc;
import com.cs324.distrilab.grpc.GetActiveWorkersRequest;
import com.cs324.distrilab.grpc.GetActiveWorkersResponse;
import com.cs324.distrilab.grpc.HeartbeatResponse;
import com.cs324.distrilab.grpc.RegisterWorkerRequest;
import com.cs324.distrilab.grpc.RegisterWorkerResponse;
import com.cs324.distrilab.grpc.WorkerHeartbeat;
import com.cs324.distrilab.grpc.WorkerInfo;

import com.cs324.distrilab.grpc.CoordinatorAnnouncement;
import com.cs324.distrilab.grpc.GetCoordinatorRequest;
import com.cs324.distrilab.grpc.GetCoordinatorResponse;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BootstrapServer
    extends BootstrapServiceGrpc.BootstrapServiceImplBase {

    private static final int PORT = 50050;

    // Stores registered workers by their worker ID.
    private final Map<Integer, WorkerInfo> workers = new ConcurrentHashMap<>();
    // Records the last heartbeat received from each worker.
    private final Map<Integer, Long> lastHeartbeat = new ConcurrentHashMap<>();
    
    private volatile int coordinatorId = -1;
    private volatile int coordinatorJac = -1;

    // Checks worker health periodically.
    private final ScheduledExecutorService monitor = 
            Executors.newSingleThreadScheduledExecutor();

    // A worker is considered inactive after 10 seconds without a heartbeat.
    private static final long HEARTBEAT_TIMEOUT_MS = 10000;
    
    // Prevent multiple automatic elections from running simultaneously.
    private final AtomicBoolean electionInProgress =
            new AtomicBoolean(false);

    // Time when the first worker registers.
    private final AtomicLong firstWorkerRegisteredAt =
            new AtomicLong(0);

    // Allow workers time to register before the initial election.
    private static final long STARTUP_GRACE_PERIOD_MS = 5000;

    @Override
    public void registerWorker(
            RegisterWorkerRequest request,
            StreamObserver<RegisterWorkerResponse> responseObserver) {

        WorkerInfo worker = request.getWorker();

       workers.put(worker.getWorkerId(),
        worker.toBuilder().setActive(true).build());

       lastHeartbeat.put(worker.getWorkerId(),
                System.currentTimeMillis());
       
       // Record when the first worker registers.
        firstWorkerRegisteredAt.compareAndSet(
                0, System.currentTimeMillis());

        System.out.println(
                "Registered Worker " + worker.getWorkerId()
                + " at " + worker.getHost()
                + ":" + worker.getPort());

        RegisterWorkerResponse response =
                RegisterWorkerResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Worker registered successfully")
                        .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getActiveWorkers(
            GetActiveWorkersRequest request,
            StreamObserver<GetActiveWorkersResponse> responseObserver) {

        GetActiveWorkersResponse.Builder response =
                GetActiveWorkersResponse.newBuilder();

        for (WorkerInfo worker : workers.values()) {
            if (worker.getActive()) {
                response.addWorkers(worker);
            }
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }
    
    @Override
public void getCoordinator(
        GetCoordinatorRequest request,
        io.grpc.stub.StreamObserver<GetCoordinatorResponse> responseObserver) {

    GetCoordinatorResponse response =
            GetCoordinatorResponse.newBuilder()
                    .setCoordinatorId(coordinatorId)
                    .setCoordinatorJac(coordinatorJac)
                    .setHasCoordinator(coordinatorId != -1)
                    .build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
}


@Override
public void announceCoordinator(
        CoordinatorAnnouncement request,
        io.grpc.stub.StreamObserver<CoordinatorAnnouncement> responseObserver) {

    coordinatorId = request.getCoordinatorId();
    coordinatorJac = request.getCoordinatorJac();

    System.out.println(
            "Bootstrap recorded coordinator: Worker "
            + coordinatorId + " (JAC " + coordinatorJac + ").");

    responseObserver.onNext(request);
    responseObserver.onCompleted();
}

@Override
public void heartbeat(
        WorkerHeartbeat request,
        StreamObserver<HeartbeatResponse> responseObserver) {

    int workerId = request.getWorkerId();

    boolean alive = workers.containsKey(workerId)
            && workers.get(workerId).getActive();

    if (alive) {
        lastHeartbeat.put(workerId, System.currentTimeMillis());
    }

    responseObserver.onNext(
            HeartbeatResponse.newBuilder()
                    .setAlive(alive)
                    .build());

    responseObserver.onCompleted();
}
 

private void triggerElection() {

    // Allow only one automatic election at a time.
    if (!electionInProgress.compareAndSet(false, true)) {
        return;
    }

    // Select an active worker to initiate the election.
    WorkerInfo initiator = workers.values().stream()
            .filter(WorkerInfo::getActive)
            .sorted((a, b) ->
                    Integer.compare(a.getWorkerId(), b.getWorkerId()))
            .findFirst()
            .orElse(null);

    if (initiator == null) {
        electionInProgress.set(false);
        System.out.println(
                "No active workers available to start an election.");
        return;
    }

    Thread electionThread = new Thread(() -> {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(initiator.getHost(), initiator.getPort())
                .usePlaintext()
                .build();

        try {
            ElectionRequest request = ElectionRequest.newBuilder()
                    .setElectionId("ELECTION-" + UUID.randomUUID())
                    .setWorkerId(initiator.getWorkerId())
                    .setJac(initiator.getJac())
                    .build();

            CoordinatorAnnouncement result =
                    WorkerServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(5, TimeUnit.SECONDS)
                            .startElection(request);

            System.out.println(
                    "Automatic election completed. Coordinator: Worker "
                    + result.getCoordinatorId());

        } catch (Exception e) {
            System.err.println(
                    "Automatic election failed: " + e.getMessage());

        } finally {
            channel.shutdownNow();
            electionInProgress.set(false);
        }
    });

    electionThread.setName("automatic-leader-election");
    electionThread.start();
}


private void monitorWorkers() {
    monitor.scheduleAtFixedRate(() -> {
        long currentTime = System.currentTimeMillis();

        // 1. Detect workers that have stopped sending heartbeats.
        for (Map.Entry<Integer, WorkerInfo> entry
                : workers.entrySet()) {

            int workerId = entry.getKey();
            WorkerInfo worker = entry.getValue();
            Long lastSeen = lastHeartbeat.get(workerId);

            if (worker.getActive()
                    && lastSeen != null
                    && currentTime - lastSeen > HEARTBEAT_TIMEOUT_MS) {

                workers.computeIfPresent(workerId, (id, latestWorker) -> {
                    if (latestWorker.getActive()) {
                        System.out.println(
                                "Worker " + id
                                + " marked INACTIVE: heartbeat timeout.");

                        return latestWorker.toBuilder()
                                .setActive(false)
                                .build();
                    }
                    return latestWorker;
                });
            }
        }

        // 2. Check whether the current coordinator is still active.
        int currentCoordinator = coordinatorId;

        if (currentCoordinator != -1) {
            WorkerInfo coordinator = workers.get(currentCoordinator);

            if (coordinator == null || !coordinator.getActive()) {
                System.out.println(
                        "Coordinator Worker " + currentCoordinator
                        + " is inactive. A new election is required.");

                coordinatorId = -1;
                coordinatorJac = -1;
            }
        }

        // 3. If no coordinator exists, trigger an election.
        boolean hasActiveWorkers = workers.values().stream()
                .anyMatch(WorkerInfo::getActive);

        long firstRegistration = firstWorkerRegisteredAt.get();

        boolean startupGracePeriodPassed =
                firstRegistration > 0
                && currentTime - firstRegistration
                        >= STARTUP_GRACE_PERIOD_MS;

        if (hasActiveWorkers
                && coordinatorId == -1
                && startupGracePeriodPassed) {

            triggerElection();
        }

    }, 3, 3, TimeUnit.SECONDS);
}

    public static void main(String[] args) {
        Server server = null;

        try {
            BootstrapServer bootstrap = new BootstrapServer();
            bootstrap.monitorWorkers();

            server = ServerBuilder.forPort(PORT)
                    .addService(bootstrap)
                    .build()
                    .start();

            System.out.println(
                    "gRPC Bootstrap Server started on port " + PORT);

            Server runningServer = server;

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Stopping Bootstrap Server...");
                bootstrap.monitor.shutdownNow();
                runningServer.shutdown();
            }));

            server.awaitTermination();

        } catch (IOException e) {
            System.err.println("Could not start Bootstrap Server.");
            e.printStackTrace();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

        } finally {
            if (server != null) {
                server.shutdown();
            }
        }
    }
}

