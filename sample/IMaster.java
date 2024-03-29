import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IMaster extends Remote {
    public Integer getRole(String name) throws RemoteException;

    public void enQueue(RequestWithTimestamp rwt) throws RemoteException;

    public RequestWithTimestamp deQueue(String name) throws RemoteException, InterruptedException;

    public void addFront() throws RemoteException;

    public void removeFront() throws RemoteException;

    public void removeMiddle() throws RemoteException;

}