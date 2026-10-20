package client;

import Interface.BootstrapInterface;
import Interface.WorkerInterface;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

/**
 *
 * @author gound
 */
public class Client {
    
     /**
     * Finds the current coordinator through
     * the Bootstrap Node.
     *
     *  WorkerInterface reference to the coordinator
     */
    private WorkerInterface findCoordinator()
            throws RemoteException {

        try {

            // Connect to the RMI registry.
            Registry registry = LocateRegistry.getRegistry("localhost",1099);

            // Find the Bootstrap Node.
            BootstrapInterface bootstrap =(BootstrapInterface)
                    registry.lookup("BootstrapNode");

            // Get all currently active workers.
            WorkerInterface[] activeWorkers = bootstrap.getActiveWorkers();

            // Check each active worker.
            for (WorkerInterface worker : activeWorkers) {

                int workerId = worker.getWorkerId();

                int coordinatorId = worker.getCoordinatorId();

                // The worker whose ID matches
                // the coordinator ID is the coordinator.
                if (workerId == coordinatorId) {

                    System.out.println(
                            "Client connected to Coordinator Worker "
                            + workerId
                    );

                    return worker;
                }
            }

            throw new RemoteException(
                    "No coordinator is currently available."
            );

        } catch (Exception e) {

            throw new RemoteException(
                    "Could not find the coordinator.",
                    e
            );
        }
    }

    /**
     * Sends a MAX job to the coordinator.
     *
     *  numbers to process
     * @return maximum number
     */
    public int submitMax(int[] numbers)
            throws RemoteException {

        WorkerInterface coordinator = findCoordinator();

        return coordinator.submitMaxJob(numbers);
    }

    /**
     * Sends a PRIMESUM job to the coordinator.
     *
     * start starting number
     * end ending number
     * return sum of prime numbers
     */
    public int submitPrimeSum(int start,int end)
            throws RemoteException {

        WorkerInterface coordinator = findCoordinator();

        return coordinator.submitPrimeSumJob(start,end);
    }

    /**
     * Sends a PRIMECOUNT job to the coordinator.
     *
     *  numbers to check
     * return number of prime numbers
     */
    public int submitPrimeCount(int[] numbers)
            throws RemoteException {

        WorkerInterface coordinator = findCoordinator();

        return coordinator.submitPrimeCountJob(numbers);
    }
}
