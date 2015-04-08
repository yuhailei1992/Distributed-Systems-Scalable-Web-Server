import java.io.File;
import java.io.Serializable;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.sql.Timestamp;

public class Server {

    public static boolean isMaster;
    /**
     * get the current timestamp as a string
     * @return
     */
    public static String getTimeStamp() {
        // get timestamp
        java.util.Date date= new java.util.Date();
        Timestamp ts = new Timestamp(date.getTime());
        return ts.toString().replaceAll("\\s+", "at");
    }

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
            master.startManager();
            master.startFront();

            master.roleQueue.add(1);
            master.roleQueue.add(1);
            //master.roleQueue.add(1);
            // start a middle layer server
            SL.startVM();
            SL.startVM();
            //SL.startVM();
            master.numVM = 2;
        } else {
            // start the corresponding thread according to the response
            Middle middle = new Middle(ip, port, SL);
            middle.startMiddle(SL);
        }
	}
}
