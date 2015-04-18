import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.util.*;
import java.io.IOException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Exchanger;

public class Master extends UnicastRemoteObject implements IMaster{

    public static final int MANAGER_COOLDOWN = 100;
    public static final int MAX_VM_NUM = 15;
    public static final int INITIAL_DROP_PERIOD = 3500;
    public static final int SCALE_IN_THRESHOLD = 80;

    public static String ip;
    public static Integer port;
    public static ServerLib SL;
    public static Integer numVM;
    public static Integer scaleInCounter;

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
        scaleInCounter = 0;

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
            Long startTime = System.currentTimeMillis();
            while (true) { // get a request, add it to the queue
                Cloud.FrontEndOps.Request r = SL.getNextRequest();
                Long currTime = System.currentTimeMillis();
                if (currTime - startTime < INITIAL_DROP_PERIOD) {
                    SL.drop(r);
                } else {
                    requestQueue.add(r);
                }
            }
        }
    }

    public synchronized void scaleOut() {
        roleQueue.add(1); // add a role
        SL.startVM(); // start a VM
        numVM++;
        System.err.println("Scaling out");
    }

    // a thread for managing
    public class Manager extends Thread {

        public Manager() throws IOException {}

        public void run(){
            System.err.println("Management thread running");
            while (true) {
                if (requestQueue.size() > (numVM + 3) && numVM < MAX_VM_NUM) {
                    System.err.println("SCALE 1");
                    // scale out
                    scaleInCounter = 0;
                    scaleOut();
                    // drop some requests.
                    int numToDrop = (requestQueue.size() - numVM) ;
                    for (int i = 0; i < numToDrop; i++) {
                        Cloud.FrontEndOps.Request r = null;
                        r = requestQueue.poll();
                        if (r != null) SL.drop(r);
                    }
                } else {
                    scaleInCounter++;
                    if (scaleInCounter > SCALE_IN_THRESHOLD) {
                        // scale in
                        numVM--;
                        scaleInCounter = 0;
                        System.err.println("Scaling in");
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
                    Thread.sleep(MANAGER_COOLDOWN);
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
