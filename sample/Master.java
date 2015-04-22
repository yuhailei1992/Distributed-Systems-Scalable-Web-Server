import java.net.MalformedURLException;
import java.rmi.*;
import java.util.*;
import java.io.IOException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Exchanger;

public class Master extends UnicastRemoteObject implements IMaster{

    public static final int MANAGER_COOLDOWN = 100;
    public static final int MAX_MIDDLE_VM_NUM = 11;
    public static final int MIN_MIDDLE_VM_NUM = 1;
    public static final int MAX_FRONT_VM_NUM = 1;
    public static final int INITIAL_DROP_PERIOD = 5000;
    public static final long PURCHASE_REQUEST_TIMEOUT = 1400;
    public static final long REGULAR_REQUEST_TIMEOUT = 600;
    public static final int SAMPLING_PERIOD = 15;
    public static float SCALE_OUT_FACTOR = 0.15f;
    public static final int FRONT_COOLDOWN = 10;

    public static int[] sampler;
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

    public static java.util.LinkedList<String> appServerQueue;

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

        System.err.println("The time is " + SL.getTime());

        sampler = new int[SAMPLING_PERIOD];

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

                sampler[samplingCounter] = requestQueue.size();
                queueLengthSum = 0;
                for (int i = 0; i < sampler.length; i++){
                    queueLengthSum += sampler[i];
                }

                if (samplingCounter == 0) {
//                    int numToStart = Math.round(((float)queueLengthSum / (float)SAMPLING_PERIOD * 2 * SCALE_OUT_FACTOR));
                    //int numToStart = Math.round(((float)queueLengthSum * SCALE_OUT_FACTOR));

                    int averageLen = queueLengthSum / SAMPLING_PERIOD;
                    int numToStart = (averageLen + 2) / 4;
                    numToStart = Math.min(numToStart, 5);

                    System.err.println("queuelength sum = " + queueLengthSum +
                            "; avglen = " + averageLen +
                            "; numMiddleVM = " + numMiddleVM +
                            "; numToStart = " + numToStart +
                            "; requestQueue size = " + requestQueue.size() +
                            "; the time = " + (System.currentTimeMillis() - startTime) +
                            "; SCALE_OUT_FACTOR = " + SCALE_OUT_FACTOR);
                    for (int i = 0; i < numToStart; i++) {
                        scaleOutMiddle();
                    }
                    try {
                        Thread.sleep(500);
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

    public class Manager2 extends Thread {

        public Manager2() throws IOException {
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


    public void scaleOutMiddle() {
        if (numMiddleVM >= MAX_MIDDLE_VM_NUM) return;
        synchronized (roleQueue) {
            roleQueue.add(1); // add a role
        }
        SL.startVM(); // start a VM
        synchronized (numMiddleVM){
            numMiddleVM++;
        }
        System.err.println("Scaling out middle, numMiddleVM = " + numMiddleVM + " Time is " + (System.currentTimeMillis() - startTime));
    }

    public synchronized void scaleOutFront() {
        if (numFrontVM >= MAX_FRONT_VM_NUM) return;
        roleQueue.add(0);
        SL.startVM();
        synchronized (numFrontVM){
            numFrontVM++;
        }
        System.err.println("Scaling out front, numFrontVM = " + numFrontVM + " Time is " + (System.currentTimeMillis() - startTime));
    }

    public synchronized void scaleInMiddle() {
        if (numMiddleVM == MIN_MIDDLE_VM_NUM) return;
        scaleInMiddleCounter = 0;

        System.err.println("Scaling in middle, numMiddleVM = " + numMiddleVM + " Time is " + (System.currentTimeMillis() - startTime));
//        String name = middleList.poll();
        String name = null;
        try {
            name = middleList.removeFirst();
        } catch (Exception e){
            System.err.println("Cannot find middle name");
        }
//        String name = middleList.removeFirst();
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

    public synchronized void scaleInFront() {

        scaleInFrontCounter = 0;
        System.err.println("Scaling in front, numFrontVM = " + numFrontVM + " Time is " + (System.currentTimeMillis() - startTime));
        //String name = frontList.poll();
        String name = null;
        try {
            name = frontList.removeFirst();
        } catch (Exception e){}
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


    public void removeMiddle() {
        if (System.currentTimeMillis() - startTime > 30000) {
            scaleInMiddle();
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
        //int role = roleQueue.poll();
        int role;
        synchronized (roleQueue) {
            role = roleQueue.removeFirst();
        }
        if (role == 0) {//front
//            System.err.println("getRole: front " + name);
            synchronized (frontList) {
                frontList.add(name);
            }
        } else {
//            System.err.println("getRole: middle" + name);
            synchronized (middleList) {
                middleList.add(name);
            }
        }
        return role;
    }

    // start a manager. only valid for master
    public void startManager() throws IOException{
        try {
            Manager manager = new Manager();
            manager.start();
            // start some middle and front
        } catch (Exception e) {
        }
    }

    public void startFront() {
        try {
            Front front = new Front();
            front.start();
        } catch (Exception e) {
        }
    }

    public void startManager2() {
        try {
            Manager2 xx = new Manager2();
            xx.start();
        } catch (Exception e) {
        }
    }

    public void setStartTime() {
        startTime = System.currentTimeMillis();
    }

    // enqueue
    public void enQueue(RequestWithTimestamp rwt) {
        synchronized(requestQueue) {
            requestQueue.add(rwt);
        }

    }

    // dequeue
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
            //rwt = requestQueue.poll();
            synchronized(requestQueue) {
                rwt = requestQueue.removeFirst();
            }

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
}
