import java.net.MalformedURLException;
import java.rmi.*;
import java.util.*;
import java.io.IOException;
import java.rmi.server.UnicastRemoteObject;

public class Master extends UnicastRemoteObject implements IMaster{

    public static final int MANAGER_COOLDOWN = 100;
    public static final int MAX_MIDDLE_VM_NUM = 13;
    public static final int MAX_FRONT_VM_NUM = 2;
    public static final int INITIAL_DROP_PERIOD = 5000;
    public static final int SCALE_IN_MIDDLE_THRESHOLD = 3000000;
    public static final long PURCHASE_REQUEST_TIMEOUT = 1400;
    public static final long REGULAR_REQUEST_TIMEOUT = 400;
    public static final float scaleOutMiddleFactor = 0.8f;
    public static long startTime = 0;

    public static Cloud.DatabaseOps cache;
    public static String ip;
    public static Integer port;
    public static ServerLib SL;
    public static Integer scaleInMiddleCounter;
    public static Integer scaleInFrontCounter;

    public static java.util.LinkedList<RequestWithTimestamp> requestQueue;
    public static java.util.LinkedList<Integer> roleQueue;
    public static HashMap<String, Integer> roleMap;
    public static java.util.LinkedList<String> middleList;
    public static java.util.LinkedList<String> frontList;

    public static Integer numMiddleVM;
    public static Integer numFrontVM;

    // bind in constructor
    public Master(String ip, int port, ServerLib SL) throws RemoteException{

        setStartTime();
        this.ip = ip;
        this.port = port;
        this.SL = SL;
        numFrontVM = 0;
        numMiddleVM = 0;
        scaleInFrontCounter = 0;
        scaleInMiddleCounter = 0;

        cache = new Cache(ip, port);
        requestQueue = new java.util.LinkedList<RequestWithTimestamp>();
        roleQueue = new java.util.LinkedList<Integer>();
        roleMap = new HashMap<String, Integer>();
        middleList = new LinkedList<String>();
        frontList = new LinkedList<String>();


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

    public void setStartTime() {
        startTime = System.currentTimeMillis();
    }
    // enqueue
    public synchronized void enQueue(RequestWithTimestamp rwt) {
        requestQueue.add(rwt);
    }

    public boolean needDropFront() {
        //System.err.println("Called needDropFront, the queue size is " + requestQueue.size() + "; number of VM is " + numVM);
        return true;
    }

    // dequeue
    public synchronized RequestWithTimestamp deQueue() {
        //RequestWithTimestamp rwt = requestQueue.poll();
        long currTime = System.currentTimeMillis();
        RequestWithTimestamp rwt;
        while (true) {
            rwt = requestQueue.poll();
            if (rwt == null) return null;
            else if (rwt.r.isPurchase && currTime - rwt.millis > PURCHASE_REQUEST_TIMEOUT) {
                SL.drop(rwt.r);
//                System.err.println("Bad purchase request, drop");
                continue;
            } else if (!rwt.r.isPurchase && currTime - rwt.millis > REGULAR_REQUEST_TIMEOUT) {
                SL.drop(rwt.r);
//                System.err.println("Bad regular request, drop");
                continue;
            } else {
                return rwt;
            }
        }
    }

    public void addFront() {
        scaleOutFront();
    }

    public void removeFront() {
        scaleInFront();
    }

    public void addMiddle() {
        scaleOutMiddle();
    }

    public void removeMiddle() {
        scaleInMiddle();
    }

    // get role
    public synchronized Integer getRole(String name) {

        int role = roleQueue.poll();
        if (role == 0) {//front
//            System.err.println("getRole::Frontlist: " + frontList);
            frontList.add(name);
        } else {
            middleList.add(name);
//            System.err.println("getRole::Middlelist: " + middleList);
        }
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
        try {
            Front front = new Front();
            front.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
                    requestQueue.add(new RequestWithTimestamp(r));
                }
            }
        }
    }

    public synchronized void scaleOutMiddle() {
        if (numMiddleVM > MAX_MIDDLE_VM_NUM) return;

        roleQueue.add(1); // add a role
        SL.startVM(); // start a VM
        numMiddleVM++;
        System.err.println("Scaling out middle, numMiddleVM is " + numMiddleVM + ", Time is " + (System.currentTimeMillis()-startTime));
    }

    public synchronized void scaleOutFront() {
        if (numFrontVM > MAX_FRONT_VM_NUM) return;
        roleQueue.add(0);
        SL.startVM();
        numFrontVM++;
        System.err.println("Scaling out front, numFrontVM is " + numFrontVM + " Time is " + (System.currentTimeMillis()-startTime));
    }

    public synchronized void scaleInMiddle() {
        scaleInMiddleCounter = 0;
        System.err.println("Scaling in middle, numMiddleVM is " + numMiddleVM + " Time is " + (System.currentTimeMillis()-startTime));
        String name = middleList.poll();
        if (name != null) {
            numMiddleVM--;
            try {
                IMiddle middle = getMiddleInstance(ip, port, name);
                middle.suicide();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void scaleInFront() {

        scaleInFrontCounter = 0;
        System.err.println("Scaling in front, numFrontVM is " + numFrontVM + " Time is " + (System.currentTimeMillis()-startTime));
        String name = frontList.poll();
        if (name != null) {
            numFrontVM--;
            try {
                IFront front = getFrontInstance(ip, port, name);
                front.suicide();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // a thread for managing
    public class Manager extends Thread {

        public Manager() throws IOException {}

        public void run(){
            System.err.println("Management thread running");
            while (true) {
                // judge if scale out middle
                if (requestQueue.size() > numMiddleVM && numMiddleVM < MAX_MIDDLE_VM_NUM) {
                    // scale out
                    scaleInMiddleCounter = 0;
                    int numToStart = Math.round((float) (requestQueue.size() - numMiddleVM) * scaleOutMiddleFactor);
//                    int numToStart = (requestQueue.size() - numMiddleVM) * 2 / 3;
                    System.err.println("need to scaleOutMiddle, the queue size is " + requestQueue.size() +
                            "; numMiddleVM is " + numMiddleVM +
                            "; numFrontVM is " + numFrontVM +
                            "; numToStart is " + numToStart);
                    for (int i = 0; i < numToStart; i++) {
                        scaleOutMiddle();
                    }
                    // drop some requests.
                    int numToDrop = (requestQueue.size() - numMiddleVM);
                    for (int i = 0; i < numToDrop; i++) {
                        Cloud.FrontEndOps.Request r;
                        r = requestQueue.poll().r;
                        if (r != null) SL.drop(r);
                    }
                } else {
                    scaleInMiddleCounter++;
                    if (scaleInMiddleCounter > SCALE_IN_MIDDLE_THRESHOLD) {
                        // scale in
                        scaleInMiddle();
                    }
                }
                // judge if scale out front

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

    public static IFront getFrontInstance(String ip, int port, String name) {
        String url = String.format("//%s:%d/%s", ip, port, name);
        try {
            return (IFront)(Naming.lookup(url));
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
