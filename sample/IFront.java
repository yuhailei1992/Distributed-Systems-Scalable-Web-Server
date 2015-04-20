import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IFront extends Remote {
    public void suicide() throws RemoteException;
}
