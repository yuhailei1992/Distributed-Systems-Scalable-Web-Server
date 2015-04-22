import java.rmi.Naming;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.sql.Timestamp;

public class Server {

    public static final int INITIAL_FRONT_LAYER = 1;

    public static int[] INITIAL = {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 4, 4, 5, 5, 4, 3, 2};

    public static boolean isMaster;
    /**
     * get the current timestamp as a string
     * @return
     */
    public static IMaster getMasterInstance(String ip, int port) {
        String url = String.format("//%s:%d/Master", ip, port);
        try {
            return (IMaster)(Naming.lookup(url));
        } catch (MalformedURLException e) {
            System.err.println("Bad URL" + e);
        } catch (RemoteException e) {
            System.err.println("Remote connection refused to url "+ url + " " + e);
        } catch (NotBoundException e) {
            System.err.println("Not bound " + e);
        }
        return null;
    }

    public static Cloud.DatabaseOps getCacheInstance(String ip, int port) {
        String url = String.format("//%s:%d/Cache", ip, port);

        try {
            return (Cloud.DatabaseOps)(Naming.lookup(url));
        } catch (MalformedURLException e) {
            System.err.println("Bad URL" + e);
        } catch (RemoteException e) {
            System.err.println("Remote connection refused to url "+ url + " " + e);
        } catch (NotBoundException e) {
            System.err.println("Not bound " + e);
        }
        return null;
    }

    public static void main (String args[]) throws Exception {

        // check parameters
        if (args.length != 2) throw new Exception("Need 2 args: <cloud_ip> <cloud_port>");
        String ip = args[0];
        int port = Integer.parseInt(args[1]);

        ServerLib SL = new ServerLib(ip, port);

        // judge if master or not
        if (SL.getStatusVM(2) == Cloud.CloudOps.VMStatus.NonExistent) {
            isMaster = true;
            System.err.println(">>>>>I am master");
        } else {
            isMaster = false;
            System.err.println(">>>>>I am slave");
        }
        // if i am master, i should start the master thread
        if (isMaster) {
            // start the master thread
            //Cache cache = new Cache(ip, port);
            Master master = new Master(ip, port, SL);
            master.startManager();
            master.startFront();
            int numToStartMiddle = INITIAL[Math.round(SL.getTime()-1)];
            for (int i = 0; i < numToStartMiddle; i++) {
                master.scaleOutMiddle();
            }
            for (int i = 0; i < INITIAL_FRONT_LAYER; i++) {
                master.scaleOutFront();
            }

            // start a middle layer server

        } else {
            IMaster master = getMasterInstance(ip, port);
            String name = getTimeStamp();
            if (master.getRole(name) == 1) {// middle
                Middle middle = new Middle(ip, port, SL, name);
                middle.startMiddle();
            } else {
                Front front = new Front(ip, port, SL, name);
                front.startFront();
            }
        }
    }

    public static synchronized String getTimeStamp() {
        // get timestamp
        java.util.Date date= new java.util.Date();
        Timestamp ts = new Timestamp(date.getTime());
        return ts.toString().replaceAll("\\s+", "at");
    }
}
