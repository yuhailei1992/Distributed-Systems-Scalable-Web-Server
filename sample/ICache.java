import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by Caesar on 4/17/15.
 */
public interface ICache extends Remote {
    public boolean hasItem(String item) throws RemoteException;
}
