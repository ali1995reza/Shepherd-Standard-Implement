package shepherd.standard.cluster.node.clusterlevelmessage;

import java.util.ArrayList;
import java.util.List;

public class  JoinResponse implements ClusterMessage {

    public final static JoinResponse AUTH_FAILED_RESPONSE =
            new JoinResponse().setType(ResponseType.AUTH_FAILED);

    public final static JoinResponse FAIL_DUE_STATE_RESPONSE =
            new JoinResponse().setType(ResponseType.FAIL_DUE_STATE);



    public enum ResponseType {
        SUCCESS, AUTH_FAILED, FAIL_DUE_STATE;

        public boolean is(ResponseType other)
        {
            return this == other;
        }
    }



    private ResponseType type;
    private SerializableNodeInfo yourInfo;
    private ArrayList<SerializableNodeInfo> nodes = new ArrayList<>();
    private String token;
    private transient ConnectRequest connectRequest;


    public JoinResponse setType(ResponseType type) {
        this.type = type;
        return this;
    }

    public JoinResponse setYourInfo(SerializableNodeInfo yourInfo) {
        this.yourInfo = yourInfo;
        return this;
    }

    public JoinResponse addNode(SerializableNodeInfo node)
    {
        nodes.add(node);
        return this;
    }

    public JoinResponse setToken(String token) {
        this.token = token;
        return this;
    }

    public ResponseType type() {
        return type;
    }

    public SerializableNodeInfo yourInfo() {
        return yourInfo;
    }

    public List<SerializableNodeInfo> nodes() {
        return nodes;
    }

    public String token() {
        return token;
    }


    public ConnectRequest toConnectRequest()
    {
        if(connectRequest==null)
            connectRequest = new ConnectRequest().setInfo(yourInfo).setToken(token);

        return connectRequest;
    }
}
