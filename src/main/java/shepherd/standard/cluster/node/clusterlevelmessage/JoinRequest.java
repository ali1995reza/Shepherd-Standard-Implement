package shepherd.standard.cluster.node.clusterlevelmessage;

import java.net.SocketAddress;

public class JoinRequest implements ClusterMessage {

    private SocketAddress nodeAddress;
    private String password;

    public JoinRequest(SocketAddress nodeAddress, String password) {
        this.nodeAddress = nodeAddress;
        this.password = password;
    }


    public JoinRequest setPassword(String password) {
        this.password = password;
        return this;
    }

    public JoinRequest setNodeAddress(SocketAddress nodeAddress) {
        this.nodeAddress = nodeAddress;
        return this;
    }

    public SocketAddress nodeAddress() {
        return nodeAddress;
    }

    public String password() {
        return password;
    }
}
