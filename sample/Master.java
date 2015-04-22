/*
 Master has several roles:
 1: a front end thread
 2: a managing thread to determine scaling out middle layer
 3: a cache
 4: a wait notify thread to coordinate middle layer servers
 */

import java.net.MalformedURLException;
import java.rmi.*;
import java.util.*;
import java.io.IOException;
import java.rmi.server.UnicastRemoteObject;

public class Master extends UnicastRemoteObject implements IMaster{

    public static final int MANAGER_COOLDOWN = 100; // managing thread cooldown
    public static final int FRONT_COOLDOWN = 10;
    public static final int MANAGER_SCALE_OUT_COOLDOWN = 650;

    public static int MAX_MIDDLE_VM_NUM = 12;
    public static final int MIN_MIDDLE_VM_NUM = 1;
    public static final int MAX_FRONT_VM_NUM = 1;

    public static final int INITIAL_DROP_PERIOD = 5000;
    public static final long PURCHASE_REQUEST_TIMEOUT = 1400;
    public static final long REGULAR_REQUEST_TIMEOUT = 500;
    public static final int SAMPLING_PERIOD = 15;

    public static Cloud.DatabaseOps cache;
    public static String ip;
    public static Integer port;
    public static ServerLib SL;
    public static long startTime; // record the time when master starts

    public static java.util.LinkedList<RequestWithTimestamp> requestQueue;
    public static java.util.LinkedList<Integer> roleQueue;
    public static HashMap<String, Integer> roleMap;
    public static java.util.LinkedList<String> middleList;
    public static java.util.LinkedList<String> frontList;
    public static java.util.LinkedList<String> appServerQueue;

    public static Integer numMiddleVM;
    public static Integer numFrontVM;

    // bind in constructor
    public Master(String ip, int port, ServerLib SL) throws RemoteException{
        System.err.println("The time is " + SL.getTime());
        startTime = System.currentTimeMillis();
        this.ip = ip;
        this.port = port;
        this.SL = SL;
        numFrontVM = 0;
        numMiddleVM = 0;

        cache = new Cache(ip, port);
        requestQueue = new java.util.LinkedList<RequestWithTimestamp>();
        appServerQueue = new LinkedList<String>();
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

    ////////////////////////////////////////////////////////////////////////////
    // SCALE OUT/IN METHODS
    ////////////////////////////////////////////////////////////////////////////

    /**
     * add a role(1) in roleQueue, and start a new server
     */
    public void scaleOutMiddle() {
        if (numMiddleVM >= MAX_MIDDLE_VM_NUM) return;
        synchronized (roleQueue) {
            roleQueue.add(1); // add a role
        }
        SL.startVM(); // start a VM
        synchronized (numMiddleVM){
            numMiddleVM++;
        }
        System.err.println("Scaling out middle, numMiddleVM = " + numMiddleVM +
                " Time is " + (System.currentTimeMillis() - startTime));
    }

    /**
     * add a role (0) in roleQueue, and start a new server
     */
    public synchronized void scaleOutFront() {
        if (numFrontVM >= MAX_FRONT_VM_NUM) return;
        roleQueue.add(0);
        SL.startVM();
        synchronized (numFrontVM){
            numFrontVM++;
        }
        System.err.println("Scaling out front, numFrontVM = " + numFrontVM +
                " Time is " + (System.currentTimeMillis() - startTime));
    }

    /**
     * get the first name in middleList, and shutdown the corresponding server
     */
    public synchronized void scaleInMiddle() {
        if (numMiddleVM == MIN_MIDDLE_VM_NUM) return;
        if (Math.round(SL.getTime()) == 18 &&
                System.currentTimeMillis() - startTime < 50) return;

        System.err.println("Scaling in middle, numMiddleVM = " + numMiddleVM +
                " Time is " + (System.currentTimeMillis() - startTime));
        String name = null;
        try {
            name = middleList.removeFirst();
        } catch (Exception e){
            System.err.println("Cannot find middle name");
        }
        if (name != null) {
            synchronized (numMiddleVM){
                numMiddleVM--;
            }
            try {
                IMiddle middle = getMiddleInstance(ip, port, name);
                middle.suicide();//shutdown app server
            } catch (Exception e) {
            }
        }
    }

    /**
     * get the first name in frontList, and shutdown the corresponding server
     */
    public synchronized void scaleInFront() {

        System.err.println("Scaling in front, numFrontVM = " + numFrontVM +
                " Time is " + (System.currentTimeMillis() - startTime));
        String name = null;
        try {
            name = frontList.removeFirst();
        } catch (Exception e){
            System.err.println("frontList is empty");
        }
        if (name != null) {
            synchronized (numFrontVM){
                numFrontVM--;
            }
            try {
                IFront front = getFrontInstance(ip, port, name);
                front.suicide();//shutdown the machine
            } catch (Exception e) {
            }
        }
    }


    /**
     * get the middle end instance by name. This is useful for shutdown app
     * server
     * @param ip
     * @param port
     * @param name
     * @return
     */
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

    /**
     * get the front end instance by name. this is useful for shutdown front end
     * server
     * @param ip
     * @param port
     * @param name
     * @return
     */
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


    /**
     * start the management thread
     * @throws IOException
     */
    public void startManager() throws IOException{
        try {
            Manager manager = new Manager();
            manager.start();
        } catch (Exception e) {
        }
    }

    /**
     * Start a front-end thread with the master
     */
    public void startFront() {
        try {
            Front front = new Front();
            front.start();
        } catch (Exception e) {
        }
    }

    /**
     * start the block and notify thread.
     */
    public void startBlockNotify() {
        try {
            BlockNotify bn = new BlockNotify();
            bn.start();
        } catch (Exception e) {
        }
    }


    ////////////////////////////////////////////////////////////////////////////
    // FRONT END THREAD
    ////////////////////////////////////////////////////////////////////////////
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
                    // drop all requests in the first 5 seconds
                    if (currTime - startTime < INITIAL_DROP_PERIOD) {
                        SL.drop(r);
                    } else {
                        requestQueue.add(new RequestWithTimestamp(r));
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

    ////////////////////////////////////////////////////////////////////////////
    // MANAGER THREAD
    ////////////////////////////////////////////////////////////////////////////

    public class Manager extends Thread {

        public int queueLengthSum;
        public int samplingCounter;
        public int[] sampler; // a sliding sampling window

        public Manager() throws IOException {
            queueLengthSum = 0;
            samplingCounter = 0;
            sampler = new int[SAMPLING_PERIOD];
        }

        public void run(){

            while (true) {
                // update the samples
                samplingCounter = (samplingCounter + 1) % SAMPLING_PERIOD;

                sampler[samplingCounter] = requestQueue.size();
                queueLengthSum = 0;
                for (int i = 0; i < sampler.length; i++){
                    queueLengthSum += sampler[i];
                }
                // after each sampling period, judge if need to scale up
                if (samplingCounter == 0) {

                    int averageLen = queueLengthSum / SAMPLING_PERIOD;
                    // calculate the number of servers to start
                    int numToStart = (averageLen + 2) / 4;
                    // cap the number with 5 to avoid scaling up too fast
                    numToStart = Math.min(numToStart, 5);

                    System.err.println("queuelength sum = " + queueLengthSum +
                            "; avglen = " + averageLen +
                            "; numMiddleVM = " + numMiddleVM +
                            "; numToStart = " + numToStart +
                            "; requestQueue size = " + requestQueue.size() +
                            "; the time = " + (System.currentTimeMillis() - startTime));
                    for (int i = 0; i < numToStart; i++) {
                        scaleOutMiddle();
                    }
                    // after scaling up, sleep for 500 ms.
                    try {
                        Thread.sleep(MANAGER_SCALE_OUT_COOLDOWN);
                    } catch (Exception e) {

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

    ////////////////////////////////////////////////////////////////////////////
    // BLOCK NOTIFY THREAD
    ////////////////////////////////////////////////////////////////////////////
    /**
     * block and notify thread.
     */
    public class BlockNotify extends Thread {

        public BlockNotify() throws IOException {
        }

        public void run() {

            while (true) {
                try {
                    Thread.sleep(10);
                } catch (Exception e) {

                }
                if (requestQueue.size() > 0) {
                    if (appServerQueue.size() > 0) {
                        String name = null;
                        synchronized (appServerQueue) {
                            if (appServerQueue.size() > 0) {
                                name = appServerQueue.remove(0);
                            }
                        }

                        synchronized (name) {
                            name.notify();
                        }
                    }
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // BELOW ARE RMIs
    ////////////////////////////////////////////////////////////////////////////

    /**
     * RMI. shutdown one app server.
     */
    public void removeMiddle() {
        if (System.currentTimeMillis() - startTime > 30000) {
            scaleInMiddle();
        }
    }

    /**
     * RMI. start one front end server.
     */
    public void addFront() {
        scaleOutFront();
    }

    /**
     * RMI. shutdown one front end server.
     */
    public void removeFront() {
        scaleInFront();
    }

    /**
     * RMI.
     * Given a name, get the currently available role, and put the name in the
     * corresponding list. The list of name is useful for scaling in.
     * @param name
     * @return
     */
    public Integer getRole(String name) {
        int role;
        synchronized (roleQueue) {
            role = roleQueue.removeFirst();
        }
        if (role == 0) {//front
            synchronized (frontList) {
                frontList.add(name);
            }
        } else {//middle
            synchronized (middleList) {
                middleList.add(name);
            }
        }
        return role;
    }


    /**
     * RMI
     * front end server calls this to add a request to the requestQueue
     * @param rwt
     */
    public void enQueue(RequestWithTimestamp rwt) {
        synchronized(requestQueue) {
            requestQueue.add(rwt);
        }

    }

    /**
     * RMI
     * middle layer calls this to get a request from requestQueue
     * @param name1
     * @return
     * @throws InterruptedException
     */
    public RequestWithTimestamp deQueue(String name1) throws InterruptedException{
        String name = name1;

        synchronized (appServerQueue) {
            appServerQueue.add(name);
        }
        synchronized (name) {
            name.wait();
        }

        long currTime = System.currentTimeMillis();
        RequestWithTimestamp rwt;
        while (true) {

            synchronized(requestQueue) {
                rwt = requestQueue.removeFirst();
            }

            if (rwt == null) return null;
            // drop the requests that are too old
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
}
