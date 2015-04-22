import java.rmi.Naming;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.sql.Timestamp;

public class Server {

    // below is the initial number of app server/ front end to start
    // we know that the load is different in different times of a day

    public static int[] FRONT =   {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0};

    public static int[] MIDDLE = {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 5, 5, 4, 3, 2};

    // tells the server whether it is a master or not. different initialization
    // procedures are taken
    public static boolean isMaster;

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
            Master master = new Master(ip, port, SL);
            // start a front end, managing thread and blocknotify thread
            master.startManager();
            master.startFront();
            master.startBlockNotify();
            // on startup, start some front/middle servers
            int time = Math.round(SL.getTime());
            int numToStartMiddle = MIDDLE[time-1];
            int numToStartFront = FRONT[time-1];
            // restrict max number of machines on 18pm
            if (time == 18) {
                Master.MAX_MIDDLE_VM_NUM = 11;
            }
            System.err.println("Need to open " + numToStartFront + "Front, " +
                    numToStartMiddle + "Middle");

            for (int i = 0; i < numToStartMiddle; i++) {
                master.scaleOutMiddle();
            }
            for (int i = 0; i < numToStartFront; i++) {
                master.scaleOutFront();
            }

        } else {
            // get the role from master, start different servers for different
            // roles
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

    /**
     * this method is used for getting a unique name for a newly spawned server
     * @return
     */
    public static synchronized String getTimeStamp() {
        // get timestamp
        java.util.Date date= new java.util.Date();
        Timestamp ts = new Timestamp(date.getTime());
        return ts.toString().replaceAll("\\s+", "at");
    }

    /**
     * get a master instance to call RMIs from master.
     * @param ip
     * @param port
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

    /**
     * get a cache instance, pass it to app servers to enable processing with
     * cache
     * @param ip
     * @param port
     * @return
     */
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
}
