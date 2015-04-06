
/* Sample code for basic Server */
import java.rmi.Naming;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

public class Server {
	private static boolean isMaster;
	public static void main ( String args[] ) throws Exception {

		// check parameters
		if (args.length != 2) throw new Exception("Need 2 args: <cloud_ip> <cloud_port>");
		ServerLib SL = new ServerLib( args[0], Integer.parseInt(args[1]) );
		
		// register with load balancer so requests are sent to this server
		SL.register_frontend();
        // SL.startVM();

		// judge if is master
		if (SL.getStatusVM(2) == Cloud.CloudOps.VMStatus.NonExistent) {
			isMaster = true;
			System.err.println(">>>>>I am master");
		} else {
			isMaster = false;
			System.err.println(">>>>>I am slave");
		}
		
		// create enough VMs
		float time = SL.getTime();

		if (isMaster) {
			if (time > 5.9 && time < 6.1) {// 6am
				for (int i = 1; i < 2; i++) {
					SL.startVM();
				}
			}
			else if (time > 7.9 && time < 8.1) {// 6am
				for (int i = 0; i < 3; i++) {
					SL.startVM();
				}
			}
			else if (time > 18.9 && time < 19.1) {// 6am
				for (int i = 0; i < 6; i++) {
					SL.startVM();
				}
			}
		}
		
		// main loop
		while (true) {
			Cloud.FrontEndOps.Request r = SL.getNextRequest();
			SL.processRequest( r );
		}
	}
}


