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
    public volatile boolean join;
    public static Cloud.DatabaseOps cache;
    public static final int MIDDLE_COOLDOWN = 1;

    /**
     * constructor, bind the object to a name
     */
    public Middle(String ip, int port, ServerLib SL, String name) throws RemoteException{
        master = Server.getMasterInstance(ip, port);
        cache = Server.getCacheInstance(ip, port);
        this.SL = SL;
        join = false;
        this.name = name;

        // register at the serverside
        try {
            Naming.bind(String.format("//%s:%d/%s", ip, port, name), this);
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
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
            e.printStackTrace();
        }
    }

    public class Processor extends Thread {
        public int scaleOutCounter = 0;
        public int scaleInCounter = 0;

        public Processor() throws IOException {
            System.err.println("Processor started");
        }

        public void run() {
            RequestWithTimestamp rwt;
            while (!join) {
                try {
                    // get a request
                    rwt = master.deQueue();
                    if (rwt != null) {
                        if (!rwt.r.isPurchase) {
                            long requestAge = (System.currentTimeMillis() - rwt.millis);
//                            if (requestAge > 300) {
//                                scaleOutCounter++;
//                                if (scaleOutCounter > 10) {
//                                    master.addMiddle();
//                                    scaleOutCounter = 0;
//                                }
//                            } else {
//                                scaleOutCounter = 0;
//                            }
                            System.err.println("Processing with cache, the elapsed time is " + requestAge);
                            SL.processRequest(rwt.r, cache);

                        } else {
                            long requestAge = (System.currentTimeMillis() - rwt.millis);
                            System.err.println("Processing with database, the elapsed time is " + requestAge);
                            SL.processRequest(rwt.r);
                        }
                    } else {
                        Thread.sleep(MIDDLE_COOLDOWN);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void suicide() {
        SL.shutDown();
        join = true;
        System.err.println("Shutting myself down");
        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
    }

}
