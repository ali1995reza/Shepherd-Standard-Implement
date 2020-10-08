package shepherd.standard.cluster.node.clusterlevelmessage;

public class ConnectRequest implements ClusterMessage {


    private String token;
    private SerializableNodeInfo info;

    public ConnectRequest setToken(String token) {
        this.token = token;
        return this;
    }

    public ConnectRequest setInfo(SerializableNodeInfo info) {
        this.info = info;
        return this;
    }

    public SerializableNodeInfo info() {
        return info;
    }

    public String token() {
        return token;
    }
}
