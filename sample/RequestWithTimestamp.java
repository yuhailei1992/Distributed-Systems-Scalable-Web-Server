import java.io.Serializable;

public class RequestWithTimestamp implements Serializable {
    public Cloud.FrontEndOps.Request r;
    public long millis;
    public RequestWithTimestamp(Cloud.FrontEndOps.Request r) {
        this.millis = System.currentTimeMillis();
        this.r = r;
    }
}