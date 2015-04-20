
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;

public class Front extends UnicastRemoteObject implements IFront {

    public static IMaster master;
    public static ServerLib SL;
    public static String name;
    public static final int FRONT_THRESHOLD = 5;
    public static final int FRONT_COOLDOWN = 10;

    /**
     * constructor, bind the object to a name
     */
    public Front(String ip, int port, ServerLib SL, String name) throws RemoteException{
        master = Server.getMasterInstance(ip, port);
        this.SL = SL;
        this.name = name;
        // register at the serverside
        System.err.println("Frontend: Done getRole");
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
     * @param SL
     */
    public void startFront() {
        try {
            FrontProcessor frontprocessor = new FrontProcessor();
            frontprocessor.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class FrontProcessor extends Thread {

        public FrontProcessor() throws IOException {
            System.err.println("Frontend Processor started");
        }

        public void run() {
            System.err.println("Front end has started");

            SL.register_frontend();

            while (true) { // get a request, add it to the queue
                try {
                    int len = SL.getQueueLength();
                    if (len > 0) {
                        Cloud.FrontEndOps.Request r = SL.getNextRequest();
                        master.enQueue(new RequestWithTimestamp(r));
                        // check if need to add front
                        if (len > FRONT_THRESHOLD) {
                            System.err.println("Front:: need to scale out. my queue len is " + len);
                            master.addFront();
                        }
                    } else {
                        Thread.sleep(FRONT_COOLDOWN);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void suicide() {
        SL.unregister_frontend();
        SL.shutDown();
        System.err.println("Frontend: Shutting myself down");
        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
    }

}

