/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Bootstrap;

import Interface.BootstrapInterface;
import Interface.WorkerInterface;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author gound
 */
public class BootstrapNode extends UnicastRemoteObject
        implements BootstrapInterface {
    
     // Stores the active workers.
    // Key = Worker ID
    // Value = Remote reference to the worker
    private final Map<Integer, WorkerInterface> activeWorkers = new HashMap<>();

    public BootstrapNode() throws RemoteException {
        super();
    }

    @Override
    public synchronized void registerWorker(int workerId,WorkerInterface worker)
            throws RemoteException {

        activeWorkers.put(workerId, worker);

        System.out.println(
                "Worker " + workerId
                + " registered with Bootstrap Node."
        );
    }

    @Override
    public synchronized WorkerInterface[] getActiveWorkers()
            throws RemoteException {

        return activeWorkers.values()
                .toArray(new WorkerInterface[0]);
    }

    @Override
    public synchronized void unregisterWorker(int workerId)
            throws RemoteException {

        activeWorkers.remove(workerId);

        System.out.println(
                "Worker " + workerId
                + " unregistered from Bootstrap Node."
        );
    }

    public static void main(String[] args) {

        try {

            BootstrapNode bootstrap = new BootstrapNode();

            Registry registry = LocateRegistry.getRegistry("localhost",1099);

            registry.rebind("BootstrapNode", bootstrap);

            System.out.println("Bootstrap Node registered in the RMI registry.");

            System.out.println("Bootstrap Node is running.");

        } catch (RemoteException e) {

            System.out.println(
                    "Could not start Bootstrap Node."
            );

            e.printStackTrace();
        }
    }
}
    
