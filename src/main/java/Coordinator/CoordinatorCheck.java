/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */


package Coordinator;

import com.cs324.distrilab.grpc.BootstrapServiceGrpc;
import com.cs324.distrilab.grpc.GetCoordinatorRequest;
import com.cs324.distrilab.grpc.GetCoordinatorResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;

public class CoordinatorCheck {

    public static void main(String[] args) {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50050)
                .usePlaintext()
                .build();

        try {
            GetCoordinatorResponse response =
                    BootstrapServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(3, TimeUnit.SECONDS)
                            .getCoordinator(
                                    GetCoordinatorRequest.newBuilder().build());

            if (response.getHasCoordinator()) {
                System.out.println("Current coordinator: Worker "
                        + response.getCoordinatorId());
                System.out.println("Coordinator JAC: "
                        + response.getCoordinatorJac());
            } else {
                System.out.println("No coordinator has been elected.");
            }

        } catch (Exception e) {
            System.err.println("Could not check coordinator: "
                    + e.getMessage());
        } finally {
            channel.shutdownNow();
        }
    }
}

