/**
 * Created by Caesar on 4/6/15.
 */

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IMaster extends Remote {
    public Integer getRole(String name) throws RemoteException;
    public boolean enQueue(Cloud.FrontEndOps.Request r) throws RemoteException;
    public Cloud.FrontEndOps.Request deQueue() throws RemoteException;

}