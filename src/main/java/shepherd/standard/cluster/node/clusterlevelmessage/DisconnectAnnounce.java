package shepherd.standard.cluster.node.clusterlevelmessage;

public class DisconnectAnnounce implements Announce {


    private SerializableNodeInfo disconnectedNode;
    private boolean left;

    public DisconnectAnnounce(SerializableNodeInfo info, boolean left)
    {
        disconnectedNode = info;
        this.left = left;
    }


    public DisconnectAnnounce setDisconnectedNode(SerializableNodeInfo disconnectedNode) {
        this.disconnectedNode = disconnectedNode;
        return this;
    }

    public void setLeft(boolean left) {
        this.left = left;
    }

    public SerializableNodeInfo disconnectedNode() {
        return disconnectedNode;
    }

    public boolean isLeft() {
        return left;
    }

    @Override
    public String toString() {
        return "DisconnectAnnounce{" +
                "disconnectedNode=" + disconnectedNode +
                ", left=" + left +
                '}';
    }
}
