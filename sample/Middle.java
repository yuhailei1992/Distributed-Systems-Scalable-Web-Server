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
    public static final int MIDDLE_COOLDOWN = 1;

    /**
     * constructor, bind the object to a name
     */
    public Middle(String ip, int port, ServerLib SL, String name) throws RemoteException{
        master = Server.getMasterInstance(ip, port);
        cache = Server.getCacheInstance(ip, port);
        this.SL = SL;
        this.name = name;

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

        public Processor() throws IOException {
            System.err.println("Processor started");
        }

        public void run() {
            RequestWithTimestamp rwt;
            while (true) {
                try {
                    // get a request
                    rwt = master.deQueue();
                    if (rwt != null) {
                        if (!rwt.r.isPurchase) {
                            System.err.println("Processing with cache, the elapsed time is " + (System.currentTimeMillis() - rwt.millis));
                            SL.processRequest(rwt.r, cache);
                        } else {
                            System.err.println("Processing with database, the elapsed time is " + + (System.currentTimeMillis() - rwt.millis));
                            SL.processRequest(rwt.r);
                        }
                    } else {
                        //Thread.sleep(MIDDLE_COOLDOWN);
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
    }

}
