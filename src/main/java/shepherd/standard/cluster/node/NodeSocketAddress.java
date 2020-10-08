package shepherd.standard.cluster.node;

import shepherd.api.cluster.node.NodeAddress;

import java.io.Serializable;
import java.net.SocketAddress;

public class NodeSocketAddress implements NodeAddress<SocketAddress> , Serializable {

    private SocketAddress address ;
    public NodeSocketAddress(SocketAddress address)
    {
        this.address = address;
    }


    @Override
    public SocketAddress address() {
        return address;
    }

    @Override
    public String toString() {

        return address==null?"null":address.toString();
    }
}
