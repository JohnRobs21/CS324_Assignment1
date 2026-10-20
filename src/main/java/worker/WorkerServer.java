/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

package worker;

import worker.WorkerImpl;
import com.cs324.distrilab.grpc.WorkerHeartbeat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.cs324.distrilab.grpc.BootstrapServiceGrpc;
import com.cs324.distrilab.grpc.RegisterWorkerRequest;
import com.cs324.distrilab.grpc.RegisterWorkerResponse;
import com.cs324.distrilab.grpc.WorkerInfo;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class WorkerServer {

    public static void main(String[] args) {
        int workerId = 1;
        int port = 50052;

        try {
            if (args.length >= 1) {
                workerId = Integer.parseInt(args[0]);
            }

            if (args.length >= 2) {
                port = Integer.parseInt(args[1]);
            }

            // STEP 1: Connect to the Bootstrap Server.
            ManagedChannel bootstrapChannel = ManagedChannelBuilder
                    .forAddress("localhost", 50050)
                    .usePlaintext()
                    .build();

            try {
                // STEP 2: Describe this worker.
                WorkerInfo workerInfo = WorkerInfo.newBuilder()
                        .setWorkerId(workerId)
                        .setJac(workerId)
                        .setHost("localhost")
                        .setPort(port)
                        .setActive(true)
                        .build();

                RegisterWorkerRequest request =
                        RegisterWorkerRequest.newBuilder()
                                .setWorker(workerInfo)
                                .build();

                // STEP 3: Register the worker.
                RegisterWorkerResponse response =
                        BootstrapServiceGrpc.newBlockingStub(bootstrapChannel)
                                .registerWorker(request);

                if (!response.getSuccess()) {
                    throw new IllegalStateException(
                            "Worker registration failed: "
                                    + response.getMessage());
                }

                System.out.println(
                        "Worker " + workerId
                                + " registered with Bootstrap Server.");

            } finally {
                bootstrapChannel.shutdown();
            }
            
            // STEP 4: Send regular heartbeats to Bootstrap.
            final int heartbeatWorkerId = workerId;
            ScheduledExecutorService heartbeatScheduler =
                    Executors.newSingleThreadScheduledExecutor();

            heartbeatScheduler.scheduleAtFixedRate(() -> {
                ManagedChannel heartbeatChannel = ManagedChannelBuilder
                        .forAddress("localhost", 50050)
                        .usePlaintext()
                        .build();

                try {
                    WorkerHeartbeat heartbeat = WorkerHeartbeat.newBuilder()
                            .setWorkerId(heartbeatWorkerId)
                            .build();

                    BootstrapServiceGrpc.newBlockingStub(heartbeatChannel)
                            .withDeadlineAfter(2, TimeUnit.SECONDS)
                            .heartbeat(heartbeat);

                } catch (Exception e) {
                    System.err.println(
                            "Worker " + heartbeatWorkerId
                                    + " could not contact Bootstrap.");

                } finally {
                    heartbeatChannel.shutdown();
                }
            }, 0, 3, TimeUnit.SECONDS);

            // STEP 5: Start the worker's task-processing server.
            Server server = ServerBuilder.forPort(port)
                    .addService(new WorkerImpl(workerId))
                    .build()
                    .start();

            System.out.println(
                    "gRPC Worker " + workerId
                            + " started on port " + port);

            final int runningWorkerId = workerId;

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println(
                        "Stopping Worker " + runningWorkerId);
                heartbeatScheduler.shutdownNow();
                server.shutdown();
            }));

            server.awaitTermination();

        } catch (Exception e) {
            System.err.println("Could not start the gRPC worker.");
            e.printStackTrace();
        }
    }
}
