import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IMaster extends Remote {
    public Integer getRole(String name) throws RemoteException;

    public void enQueue(RequestWithTimestamp rwt) throws RemoteException;

    public RequestWithTimestamp deQueue() throws RemoteException;

    public boolean needDropFront() throws RemoteException;

    public void addFront() throws RemoteException;
    public void removeFront() throws RemoteException;
}