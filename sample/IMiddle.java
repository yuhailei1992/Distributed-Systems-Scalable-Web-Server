/**
 * Created by Caesar on 4/6/15.
 */

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IMiddle extends Remote {
    public boolean suicide() throws RemoteException;
}