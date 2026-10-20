/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

package Coordinator;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;

public class CoordinatorServer {

    private static final int PORT = 50051;

    public static void main(String[] args) {

        Server server = null;

        try {
            server = ServerBuilder.forPort(PORT)
                    .addService(new JobServiceImpl())
                    .build()
                    .start();

            System.out.println(
                    "gRPC Coordinator Server started on port "
                    + PORT
            );

            Server runningServer = server;

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Stopping gRPC Coordinator Server...");
                runningServer.shutdown();
            }));

            server.awaitTermination();

        } catch (IOException e) {
            System.err.println("Could not start the gRPC server.");
            e.printStackTrace();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("The gRPC server was interrupted.");

        } finally {
            if (server != null) {
                server.shutdown();
            }
        }
    }
}