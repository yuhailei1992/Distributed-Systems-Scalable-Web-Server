import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IMiddle extends Remote {
    public void suicide() throws RemoteException;
}
