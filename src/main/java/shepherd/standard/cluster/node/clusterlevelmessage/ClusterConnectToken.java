package shepherd.standard.cluster.node.clusterlevelmessage;



public class ClusterConnectToken implements ClusterMessage {


    private String token;
    private SerializableNodeInfo info;
    private long expireTimeOut = 10000; // as milliseconds - default is 10 seconds !
    private long createdTime;


    public ClusterConnectToken setToken(String token) {
        this.token = token;
        return this;
    }

    public ClusterConnectToken setInfo(SerializableNodeInfo info) {
        this.info = info;
        return this;
    }

    public ClusterConnectToken setExpireTimeOut(long expireTimeOut) {
        this.expireTimeOut = expireTimeOut;
        return this;
    }

    public ClusterConnectToken setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
        return this;
    }

    public long createdTime() {
        return createdTime;
    }

    public long expireTimeOut() {
        return expireTimeOut;
    }

    public SerializableNodeInfo info() {
        return info;
    }

    public String token() {
        return token;
    }


    public boolean validate(ConnectRequest request)
    {
        if(request==null)
            return false;

        return info.equals(request.info()) && token.equals(request.token());
    }
}
