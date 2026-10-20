/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */


package Election;

import com.cs324.distrilab.grpc.CoordinatorAnnouncement;
import com.cs324.distrilab.grpc.ElectionRequest;
import com.cs324.distrilab.grpc.WorkerServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class ElectionTest {

    public static void main(String[] args) {

        // Worker 1 is listening on port 50052.
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50053)
                .usePlaintext()
                .build();

        try {
            String electionId = "ELECTION-" + System.currentTimeMillis();

            ElectionRequest request = ElectionRequest.newBuilder()
                    .setElectionId(electionId)
                    .setWorkerId(1)
                    .setJac(1)
                    .build();

            System.out.println("Starting leader election...");

            CoordinatorAnnouncement result =
                    WorkerServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(10, TimeUnit.SECONDS)
                            .startElection(request);

            System.out.println("\n===== ELECTION RESULT =====");
            System.out.println("Election ID: "
                    + result.getElectionId());
            System.out.println("Coordinator ID: "
                    + result.getCoordinatorId());
            System.out.println("Coordinator JAC: "
                    + result.getCoordinatorJac());

            if (result.getCoordinatorId() == 1) {
                System.out.println("SUCCESS: Worker 1 was elected.");
            } else {
                System.out.println("Election completed, but another "
                        + "worker was elected.");
            }

        } catch (Exception e) {
            System.err.println("Election test failed: "
                    + e.getMessage());
            e.printStackTrace();

        } finally {
            channel.shutdownNow();
        }
    }
}
