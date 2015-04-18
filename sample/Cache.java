import Cloud.DatabaseOps;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

public class Cache extends UnicastRemoteObject implements DatabaseOps {
    public static HashMap<String, String> DB;
    public static ServerLib SL;
    private String authString;
    // seems that the cache doesn't need auth, because it doesn't handle purchases
    public Cache(String ip, int port) throws RemoteException {
        // bind
        try {
            Naming.bind(String.format("//%s:%d/Cache", ip, port), this);
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        // create a cache
        this.DB = new HashMap();
        // create a serverlib
        this.SL = new ServerLib(ip, port);
        System.err.println("Created a cache");
    }


    public synchronized boolean hasItem(String item) throws RemoteException {
        if (this.DB.containsKey(item)) {
            return true;
        }
        return false;
    }

    public synchronized void shutDown() throws RemoteException {
        UnicastRemoteObject.unexportObject(this, true);
    }


    public synchronized String get(String key) throws RemoteException {
        key = key.trim();
        if (this.DB.containsKey(key)) {
            return this.DB.get(key);
        } else {
            // get from db, and write into the hashmap
            String value = SL.getDB().get(key);
            this.DB.put(key, value.trim());
            return value.trim();
        }
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

}
