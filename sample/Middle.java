/*
  Middle layer refers to app servers
  The middle server, once started by server, will create a processing thread.
  The process thread continuously get request from master's requestQueue, and
  process the requests with cache or with database.

  There are two contants for tuning. One is SAMPLING_PERIOD, which refers to how
  many sample a middle layer needs to make decision to scale in.
  SCALE_IN_THRESHOLD is the threshold for scaling in.
 */

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;

public class Middle extends UnicastRemoteObject implements IMiddle{

    public static IMaster master; // each server has a master instance
    public static ServerLib SL; // each server has a SL
    public static String name; // each server has a name

    public static Cloud.DatabaseOps cache; // cache interface

    public static final int SCALE_IN_THRESHOLD =1300;
    public static final int SAMPLING_PERIOD = 7;

    public Middle(String ip, int port, ServerLib SL, String name1)
            throws RemoteException{
        master = Server.getMasterInstance(ip, port);
        cache = Server.getCacheInstance(ip, port);
        this.SL = SL;
        this.name = name1;
        // bind the object to a name
        try {
            Naming.bind(String.format("//%s:%d/%s", ip, port, name), this);
        } catch (AlreadyBoundException e) {
        } catch (RemoteException e) {
        } catch (MalformedURLException e) {
        }
    }

    /**
     * start a thread of middle layer(app server)
     * @param
     */
    public void startMiddle() {
        try {
            Processor processor = new Processor();
            processor.run();
        } catch (Exception e) {
        }
    }

    /**
     * the middle layer (app server) thread
     */
    public class Processor extends Thread {

        int samplingCounter; // the sampling counter
        long waitTimeSum; // the time it takes to get N requests from master
        long prevTime; // the end of previous sampling period

        public Processor() throws IOException {
            samplingCounter = 0;
            waitTimeSum = 0;
            prevTime = System.currentTimeMillis();
        }

        public void run() {

            RequestWithTimestamp rwt;

            while (true) {

                try {
                    // get a request, record the time waiting for a request
                    long before = System.currentTimeMillis();
                    rwt = master.deQueue(name);
                    long after = System.currentTimeMillis();
                    waitTimeSum += (after - before);

                    if (rwt != null) {

                        if (!rwt.r.isPurchase) {
                            SL.processRequest(rwt.r, cache);
                        } else {
                            SL.processRequest(rwt.r);
                        }

                        // cyclic counter
                        samplingCounter = (samplingCounter + 1) % SAMPLING_PERIOD;
                        // samplingCounter = 0, we have a full sampling period
                        if (samplingCounter == 0) {
                            // if the middle server waits too long
                            // for a request, it should scale in
                            if (waitTimeSum > SCALE_IN_THRESHOLD) {
                                System.err.println("Scale in. sum time is " +
                                        (System.currentTimeMillis() - prevTime));
                                master.removeMiddle();
                            }
                            waitTimeSum = 0;
                        }
                    } else {

                    }
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * RMI. The master calls this method to shutdown an app server.
     */
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
