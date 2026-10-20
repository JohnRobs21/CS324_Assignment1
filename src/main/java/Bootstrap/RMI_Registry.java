/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Bootstrap;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 *
 * @author gound
 */

/**
 * Starts the Java RMI Registry used by the Worker nodes.
 *
 * The registry allows workers to find and communicate
 * with other workers through Java RMI.
 */
public class RMI_Registry {
     public static void main(String[] args) {

        try {

            // Create the RMI registry on port 1099.
            Registry registry = LocateRegistry.createRegistry(1099);

            System.out.println(
                    "RMI Registry started on port 1099."
            );
            
            System.in.read();

        } catch (Exception e) {

            System.out.println(
                    "Could not start the RMI Registry."
            );

            e.printStackTrace();
        }
    }
}
