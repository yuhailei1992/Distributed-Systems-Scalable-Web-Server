import Cloud.DatabaseOps;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

public class Cache extends UnicastRemoteObject implements DatabaseOps {
    private final HashMap<String, String> DB;
    private final String authString;

    public Cache(String auth) throws RemoteException {
        super(0);
        this.DB = new HashMap();
        this.authString = auth;
    }

    /*
    public Cache(Database db, String auth) throws RemoteException {
        super(0);
        this.DB = new HashMap(db.DB);
        this.authString = auth;
    }
    */


    public synchronized void shutDown() throws RemoteException {
        UnicastRemoteObject.unexportObject(this, true);
    }


    public synchronized String get(String key) throws RemoteException {

        return this.DB.get(key.trim());
    }

    public synchronized boolean set(String item, String value, String auth) throws RemoteException {
        if(!auth.equals(this.authString)) {
            return false;
        } else {
            this.DB.put(item.trim(), value.trim());
            return true;
        }
    }

    public synchronized boolean transaction(String key, float price, int qty) throws RemoteException {
        String item = key.trim();
        String value = this.DB.get(item);
        if(value != null && value.equals("ITEM")) {
            if(Float.parseFloat(this.DB.get(item + "_price")) != price) {
                return false;
            } else {
                int inventory = Integer.parseInt(this.DB.get(item + "_qty"));
                if(qty >= 1 && inventory >= qty) {
                    inventory -= qty;
                    this.DB.put(item + "_qty", "" + inventory);
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    /*
      how to register the db as a rmi?
     */
    public static IMiddle getDatabaseInstance(String ip, int port, String name) {
        String url = String.format("//%s:%d/%s", ip, port, name);
        try {
            return (IMiddle)(Naming.lookup(url));
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
