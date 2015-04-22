/*
 On startup, the middle layer should get a db cache
 */

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;

public class Middle extends UnicastRemoteObject implements IMiddle{

    public static IMaster master;
    public static ServerLib SL;
    public static String name;
    public static Cloud.DatabaseOps cache;
    public int scaleInCounter;
    public static final int SCALE_IN_THRESHOLD =1400;
    public static final int SAMPLING_PERIOD = 7;

    /**
     * constructor, bind the object to a name
     */
    public Middle(String ip, int port, ServerLib SL, String name1) throws RemoteException{
        master = Server.getMasterInstance(ip, port);
        cache = Server.getCacheInstance(ip, port);
        this.SL = SL;
        this.name = name1;
        scaleInCounter = 0;

        // register at the serverside
        try {
            Naming.bind(String.format("//%s:%d/%s", ip, port, name), this);
        } catch (AlreadyBoundException e) {
        } catch (RemoteException e) {
        } catch (MalformedURLException e) {
        }
    }

    /**
     * start a thread of middle
     * @param
     */
    public void startMiddle() {
        try {
            Processor processor = new Processor();
            processor.run();
        } catch (Exception e) {
        }
    }

    public class Processor extends Thread {

        public long prevTime;
        public int cnt;
        long sum;

        public Processor() throws IOException {
//            System.err.println("Middle layer started");
            prevTime = System.currentTimeMillis();
            cnt = 0;
            sum = 0;
        }

        public void run() {

            RequestWithTimestamp rwt;

            while (true) {

                try {
                    // get a request
                    long a = System.currentTimeMillis();
                    rwt = master.deQueue(name);
                    long b = System.currentTimeMillis();
                    long elapsed = b - a;
                    sum += elapsed;

                    if (rwt != null) {

//                        System.err.println("New request, time is " + (System.currentTimeMillis() - prevTime) +
//                        "\t cnt is " + cnt);

                        if (!rwt.r.isPurchase) {
//                            System.err.println("Processing with cache, the elapsed time is " + (System.currentTimeMillis() - rwt.millis));
                            SL.processRequest(rwt.r, cache);
                        } else {
//                            System.err.println("Processing with database, the elapsed time is " + + (System.currentTimeMillis() - rwt.millis));
                            SL.processRequest(rwt.r);
                        }

                        cnt = (cnt + 1) % SAMPLING_PERIOD;
                        if (cnt == 0) {
                            if (sum > SCALE_IN_THRESHOLD) {
                                System.err.println("Need to scale in. currtime is " + (System.currentTimeMillis() - prevTime));
                                master.removeMiddle();
//                                if (sum > SCALE_IN_THRESHOLD * 3/2) {
//                                    master.removeMiddle();
//                                }
                            }
                            sum = 0;
                        }
                    } else {

                    }
                } catch (Exception e) {
                }
            }
        }
    }

    public void suicide() {
        SL.shutDown();
        System.err.println("Shutting myself down");
        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (NoSuchObjectException e) {
        }
        System.exit(1);
    }

}
