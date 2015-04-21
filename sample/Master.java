import org.omg.CORBA.INITIALIZE;

import java.net.MalformedURLException;
import java.rmi.*;
import java.util.*;
import java.io.IOException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Exchanger;

public class Master extends UnicastRemoteObject implements IMaster{

    public static final int MANAGER_COOLDOWN = 100;
    public static final int MAX_MIDDLE_VM_NUM = 10;
    public static final int MAX_FRONT_VM_NUM = 1;
    public static final int INITIAL_DROP_PERIOD = 5000;
    public static final int SCALE_IN_MIDDLE_THRESHOLD = 50;
    public static final int SCALE_IN_FRONT_THRESHOLD = 100;
    public static final long PURCHASE_REQUEST_TIMEOUT = 1200;
    public static final long REGULAR_REQUEST_TIMEOUT = 200;
    public static final int SAMPLING_PERIOD = 5;

    public static int MAX_BATCH_NEW_MACHINE = 5;

    public static float SCALE_OUT_FACTOR = 1.0f;

    public static final int FRONT_THRESHOLD = 5;
    public static final int FRONT_COOLDOWN = 10;

    public static Cloud.DatabaseOps cache;
    public static String ip;
    public static Integer port;
    public static ServerLib SL;
    public static Integer scaleInMiddleCounter;
    public static Integer scaleInFrontCounter;

    public static long startTime;

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
            System.err.println("Already bound");
        } catch (RemoteException e) {
        } catch (MalformedURLException e) {
        }
    }

    public void setStartTime() {
        startTime = System.currentTimeMillis();
    }

    // enqueue
    public void enQueue(RequestWithTimestamp rwt) {
        requestQueue.add(rwt);
    }

    public boolean needDropFront() {
        //System.err.println("Called needDropFront, the queue size is " + requestQueue.size() + "; number of VM is " + numVM);
        return true;
    }

    // dequeue
    public RequestWithTimestamp deQueue() {
        //RequestWithTimestamp rwt = requestQueue.poll();
        long currTime = System.currentTimeMillis();
        RequestWithTimestamp rwt;
        while (true) {
            rwt = requestQueue.poll();
            if (rwt == null) return null;
            else if (rwt.r.isPurchase && currTime - rwt.millis > PURCHASE_REQUEST_TIMEOUT) {
                SL.drop(rwt.r);
                continue;
            } else if (!rwt.r.isPurchase && currTime - rwt.millis > REGULAR_REQUEST_TIMEOUT) {
                SL.drop(rwt.r);
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

    // get role
    public Integer getRole(String name) {
        int role = roleQueue.poll();
        if (role == 0) {//front
            frontList.add(name);
        } else {
            middleList.add(name);
        }
        return role;
    }

    // start a manager. only valid for master
    public void startManager() throws IOException{
        try {
            Manager manager = new Manager();
            manager.start();
        } catch (Exception e) {
        }
    }

    public void startFront() throws IOException{
        try {
            Front front = new Front();
            front.start();
        } catch (Exception e) {
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
                int len = SL.getQueueLength();
                if (len > 0) {
                    long currTime = System.currentTimeMillis();
                    Cloud.FrontEndOps.Request r = SL.getNextRequest();
                    if (currTime - startTime < INITIAL_DROP_PERIOD) {
                        SL.drop(r);
                    } else {
                        requestQueue.add(new RequestWithTimestamp(r));
                    }

                    if (len > FRONT_THRESHOLD) {
                        System.err.println("Front:: need to scale out. my queue len is " + len);
                        addFront();
                    }
                } else {
                    try {
                        Thread.sleep(FRONT_COOLDOWN);
                    } catch (Exception e) {
                        System.err.println("Front: sleep interrupted");
                    }
                }
            }
        }
    }

    public void scaleOutMiddle() {
        if (numMiddleVM > MAX_MIDDLE_VM_NUM) return;

        roleQueue.add(1); // add a role
        SL.startVM(); // start a VM
        synchronized (numMiddleVM){
            numMiddleVM++;
        }
        System.err.println("Scaling out middle, numMiddleVM = " + numMiddleVM + " Time is " + (System.currentTimeMillis() - startTime));
    }

    public void scaleOutFront() {
        if (numFrontVM > MAX_FRONT_VM_NUM) return;
        roleQueue.add(0);
        SL.startVM();
        synchronized (numFrontVM){
            numFrontVM++;
        }
        System.err.println("Scaling out front, numFrontVM = " + numFrontVM + " Time is " + (System.currentTimeMillis() - startTime));
    }

    public void scaleInMiddle() {
        scaleInMiddleCounter = 0;
        System.err.println("Scaling in middle, numMiddleVM = " + numMiddleVM + " Time is " + (System.currentTimeMillis() - startTime));
        String name = middleList.poll();
        if (name != null) {
            synchronized (numMiddleVM){
                numMiddleVM--;
            }
            try {
                IMiddle middle = getMiddleInstance(ip, port, name);
                middle.suicide();
            } catch (Exception e) {
            }
        }
    }

    public void scaleInFront() {

        scaleInFrontCounter = 0;
        System.err.println("Scaling in front, numFrontVM = " + numFrontVM + " Time is " + (System.currentTimeMillis() - startTime));
        String name = frontList.poll();
        if (name != null) {
            synchronized (numFrontVM){
                numFrontVM--;
            }
            try {
                IFront front = getFrontInstance(ip, port, name);
                front.suicide();
            } catch (Exception e) {
            }
        }
    }

    // a thread for managing
    public class Manager extends Thread {

        public int queueLengthSum;
        public int samplingCounter;

        public Manager() throws IOException {
            queueLengthSum = 0;
            samplingCounter = 0;
        }

        public void run(){

            while (true) {
                // judge if scale out middle
                samplingCounter = (samplingCounter + 1) % SAMPLING_PERIOD;
                queueLengthSum += requestQueue.size();
                if (samplingCounter == 4) {
                    int numToStart = Math.round(((float)queueLengthSum / (float)SAMPLING_PERIOD / (float)numMiddleVM) * 2 * SCALE_OUT_FACTOR);

                    numToStart = Math.max(numToStart, MAX_BATCH_NEW_MACHINE);

                    System.err.println("queuelength sum = " + queueLengthSum +
                            "; numToStart = " + numToStart +
                            "; numMiddleVM = " + numMiddleVM +
                            "; requestQueue size = " + requestQueue.size() +
                            "; the time = " + (System.currentTimeMillis() - startTime) +
                            "; SCALE_OUT_FACTOR = " + SCALE_OUT_FACTOR);
                    queueLengthSum = 0; // clear the sum
                    //&& ((System.currentTimeMillis() - startTime) > 5000)
                    if (numToStart > 2 && numMiddleVM < MAX_MIDDLE_VM_NUM) {
                        System.err.println("Decided to scale up! ");
                        for (int i = 0; i < numToStart; i++) {
                            scaleOutMiddle();
                        }

//                        SCALE_OUT_FACTOR *= 0.95;

                        try {
                            Thread.sleep(5000);
                        } catch (Exception e) {
                        }
                    } else {
                        scaleInMiddleCounter++;
                        if (scaleInMiddleCounter > SCALE_IN_MIDDLE_THRESHOLD) {
                            scaleInMiddle();
                            scaleInMiddleCounter = 0;
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
