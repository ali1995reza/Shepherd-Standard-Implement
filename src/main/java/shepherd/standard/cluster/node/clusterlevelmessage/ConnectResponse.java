package shepherd.standard.cluster.node.clusterlevelmessage;

public class ConnectResponse implements ClusterMessage {


    private SerializableNodeInfo info;
    private boolean success;
    private String hashId;


    public ConnectResponse setInfo(SerializableNodeInfo info) {
        this.info = info;
        return this;
    }

    public ConnectResponse setSuccess(boolean success) {
        this.success = success;
        return this;
    }

    public SerializableNodeInfo info() {
        return info;
    }

    public boolean isSuccess() {
        return success;
    }

    public ConnectResponse setHashId(String hashId) {
        this.hashId = hashId;
        return this;
    }

    public String hashId() {
        return hashId;
    }
}
