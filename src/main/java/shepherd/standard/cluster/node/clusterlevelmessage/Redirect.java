package shepherd.standard.cluster.node.clusterlevelmessage;

import java.net.SocketAddress;

@Deprecated
public class Redirect implements ClusterMessage {


    private SocketAddress address;

    public Redirect(SocketAddress address)
    {
        this.address = address;
    }

    public Redirect setAddress(SocketAddress address) {
        this.address = address;
        return this;
    }

    public SocketAddress address() {
        return address;
    }
}
