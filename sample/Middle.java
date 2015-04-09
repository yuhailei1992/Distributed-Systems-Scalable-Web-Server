import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;

public class Middle extends UnicastRemoteObject {

    public static IMaster master;
    public static ServerLib SL;
    public static String name;
    public static final int MIDDLE_COOLDOWN = 1;

    /**
     * constructor, bind the object to a name
     */
    public Middle(String ip, int port, ServerLib SL) throws RemoteException{
        master = Server.getMasterInstance(ip, port);
        this.SL = SL;
        this.name = getTimeStamp();
        // register at the serverside
        master.getRole(this.name);
        System.err.println("Done getRole");
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
    public void startMiddle(ServerLib SL) {
        try {
            Processor processor = new Processor();
            processor.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class Processor extends Thread {

        public Processor() throws IOException {
            System.err.println("Processor started");
        }

        public void run() {
            while (true) {
                Cloud.FrontEndOps.Request r = null;
                try {
                    // get a request
                    r = master.deQueue();
                    if (r != null) {
                        SL.processRequest(r);
                        // System.err.println("Done with one request");
                    }
                    //else Thread.sleep(MIDDLE_COOLDOWN);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public boolean killSelf() {
        SL.shutDown();
        System.err.println("Shutting myself down");
        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static synchronized String getTimeStamp() {
        // get timestamp
        java.util.Date date= new java.util.Date();
        Timestamp ts = new Timestamp(date.getTime());
        return ts.toString().replaceAll("\\s+", "at");
    }

}
