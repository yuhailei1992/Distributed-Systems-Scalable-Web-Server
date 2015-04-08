import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.util.*;
import java.io.IOException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Exchanger;

public class Master extends UnicastRemoteObject implements IMaster{

    public static String ip;
    public static Integer port;
    public static ServerLib SL;
    public static Integer numVM;
    public static Integer scaleDownCount;

    public static java.util.LinkedList<Cloud.FrontEndOps.Request> requestQueue;
    public static java.util.LinkedList<Integer> roleQueue;
    public static HashMap<String, Integer> roleMap;
    public static java.util.LinkedList<String> middleList;

    // bind in constructor
    public Master(String ip, int port, ServerLib SL) throws RemoteException{

        this.ip = ip;
        this.port = port;
        this.SL = SL;
        numVM = 0;
        scaleDownCount = 0;

        requestQueue = new java.util.LinkedList<Cloud.FrontEndOps.Request>();
        roleQueue = new java.util.LinkedList<Integer>();
        roleMap = new HashMap<String, Integer>();
        middleList = new LinkedList<String>();

        try {
            Naming.bind(String.format("//%s:%d/Master", ip, port), this);
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    // enqueue
    public synchronized boolean enQueue(Cloud.FrontEndOps.Request r) {
        requestQueue.add(r);
        return true;
    }

    // dequeue
    public synchronized Cloud.FrontEndOps.Request deQueue() {
        return requestQueue.poll();
    }

    // get role
    public synchronized Integer getRole(String name) {
        int role = roleQueue.poll();
        roleMap.put(name, role); // record the mapping
        return role;
    }

    // start a manager. only valid for master
    public void startManager() throws IOException{
        try {
            Manager manager = new Manager();
            manager.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startFront() throws IOException{
        Front front = new Front();
        front.start();
    }

    // a thread for frontend
    public class Front extends Thread {

        public Front() throws IOException {}

        public void run() {
            System.err.println("Front end has started");
            SL.register_frontend();
            Long starttime = System.currentTimeMillis();
            while (true) { // get a request, add it to the queue
                Cloud.FrontEndOps.Request r = SL.getNextRequest();
                /*if (SL.getStatusVM(2) == Cloud.CloudOps.VMStatus.Booting) {
                    SL.drop(r);
                } else {
                    requestQueue.add(r);
                }*/
                Long currtime = System.currentTimeMillis();
                if (currtime - starttime < 3000) {
                    SL.drop(r);
                } else {
                    requestQueue.add(r);
                }
            }
        }
    }

    public synchronized void scaleUp() {
        roleQueue.add(1); // add a role
        SL.startVM(); // start a VM
        System.err.println("Scaling up");
    }

    // a thread for managing
    public class Manager extends Thread {

        public Manager() throws IOException {}

        public void run(){
            System.err.println("Management thread running");
            while (true) {
                if (requestQueue.size() > numVM && numVM < 14) {
                    // scale up
                    scaleDownCount = 0;
                    numVM++;
                    scaleUp();

                    /*try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }*/

                } else {
                    scaleDownCount++;
                    if (scaleDownCount > 100) {
                        // scale down
                        numVM--;
                        scaleDownCount = 0;
                        System.err.println("Scaling down");
                        String name = middleList.poll();
                        if (name != null) {
                            IMiddle middle = getMiddleInstance(ip, port, name);
                            try {
                                middle.killSelf();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }

                try{
                    Thread.sleep(250);
                }catch(InterruptedException e){
                    System.err.println("Got interrupted!");
                }

            }
        }
    }


    public static IMiddle getMiddleInstance(String ip, int port, String name) {
        String url = String.format("//%s:%d/%s", ip, port, name);
        try {
            return (IMiddle)(Naming.lookup(url));
        } catch (MalformedURLException e) {
            System.err.println("Bad URL" + e);
        } catch (RemoteException e) {
            System.err.println("Remote connection refused to url "+ url + " " + e);
        } catch (NotBoundException e) {
            System.err.println("Not bound " + e);
        }
        return null;
    }

}
