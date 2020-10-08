package shepherd.standard.cluster.node.clusterlevelmessage;

import shepherd.standard.utils.Hash;

public class ConnectAnnounce implements Announce {




    private SerializableNodeInfo connectedNode;
    private final String hashId;


    public ConnectAnnounce(SerializableNodeInfo info)
    {
        connectedNode = info;
        hashId = Hash.generateSecureHashString();
    }


    public ConnectAnnounce setConnectedNode(SerializableNodeInfo connectedNode) {
        this.connectedNode = connectedNode;
        return this;
    }

    public SerializableNodeInfo connectedNode() {
        return connectedNode;
    }

    public String hashId()
    {
        return hashId;
    }
}
