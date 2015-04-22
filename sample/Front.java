/*
 The front end server will start a thread that consistently get request, add
 a timestamp and push it to the master's requestQueue.

 If the front end failed to get a request by SL.getNextRequest, it will sleep
 for a period.
 */
import java.io.IOException;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;

public class Front extends UnicastRemoteObject implements IFront {

    public static IMaster master; // each server has a master instance
    public static ServerLib SL; // each server has a SL
    public static String name; // each server has a name

    public static final int FRONT_THRESHOLD = 7; // threshold to sale out
    public static final int FRONT_COOLDOWN = 50; // sampling period
    public static final int SCALE_IN_THRESHOLD = 10000; // threshold to scale in

    public Front(String ip, int port, ServerLib SL, String name) throws RemoteException{
        master = Server.getMasterInstance(ip, port);
        this.SL = SL;
        this.name = name;
        // bind to a name
        try {
            Naming.bind(String.format("//%s:%d/%s", ip, port, name), this);
        } catch (Exception e) {
        }
    }

    /**
     * start a thread of front end server
     * @param
     */
    public void startFront() {
        try {
            FrontProcessor frontprocessor = new FrontProcessor();
            frontprocessor.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * front end processor. get requests, add timestamp, and push it into
     * the requestQueue in master
     */
    public class FrontProcessor extends Thread {

        public int scaleInCounter;

        public FrontProcessor() throws IOException {
            System.err.println("Frontend Processor started");
        }

        public void run() {
            System.err.println("Front end has started");

            SL.register_frontend();
            scaleInCounter = 0;

            while (true) { // get a request, add it to the queue
                try {
                    int len = SL.getQueueLength();
                    if (len > 0) { // decide if scale out or not
                        Cloud.FrontEndOps.Request r = SL.getNextRequest();
                        master.enQueue(new RequestWithTimestamp(r));
                        if (len > FRONT_THRESHOLD) { // scale out
                            System.err.println("Front:: need to scale out. my queue len is " + len);
                            master.addFront();
                        }
                    } else { // decide if scale in or not
                        scaleInCounter++;
                        if (scaleInCounter > SCALE_IN_THRESHOLD) { // scale in
                            master.removeFront();
                            scaleInCounter = 0;
                        }

                        Thread.sleep(FRONT_COOLDOWN);

                    }
                } catch (Exception e) {
                    System.err.println("Error in front processor thread");
                }
            }
        }
    }

    /**
     * RMI. Master calls this method to shutdown a front end server
     */
    public void suicide() {
        SL.unregister_frontend();
        SL.shutDown();
        System.err.println("Frontend: Shutting myself down");
        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (NoSuchObjectException e) {
            System.err.println("Error in front end suicide method");
        }
    }

}

