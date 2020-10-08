package shepherd.standard.cluster.node;

import shepherd.standard.cluster.node.clusterlevelmessage.JoinResponse;
import shepherd.standard.message.standardserializer.AppenderSerializer;
import shepherd.standard.message.standardserializer.ObjectSerializer;
import shepherd.api.cluster.node.NodeInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;


final class ClusterProtocolConstants {

    public final static class MessageTypes
    {
        public final static byte UNICAST  = 0;
        public final static byte MULTICAST = 1;
        public final static byte BROADCAST = 2;
        public final static byte CLUSTER_MESSAGE = 3;
        public final static byte ACKNOWLEDGE = 4;
        public final static byte ALIVE_REQUEST = 5;
        public final static byte ALIVE_RESPONSE = 6;
    }




    public final static class ClusterMessageTypes
    {
        public final static byte CLUSTER_INFORMATION = 1;
        public final static byte LEADER_ADDRESS = 2;
        public final static byte AUTH_TOKEN = 3;
        public final static byte JOIN_REQUEST = 4;
        public final static byte AUTH_FAILED = 5;
        public final static byte READY = 6;
        public final static byte PRESENTATION = 7;
        public final static byte SET_CONFIGURATION = 8;
        public final static byte LEAVING = 9;
        public final static byte YOUR_LEAVE_FLAG_SET = 10;
    }

    public final static class ClusterMessageKeys
    {
        public final static String TYPE = "T";
        public final static String NODES_INFORMATION = "NI";
            public final static String IP = "I";
            public final static String PORT = "P";
            public final static String ID = "ID";
            public final static String LEADER = "L";
            public final static String JOIN_TIME = "JT";
            public final static String YOUR_INFORMATION  = "YI";
        public final static String AUTH_TOKEN = "AT";
        public final static String PASSWORD = "PSWRD";

    }




    private static final Base64.Encoder encoder = Base64.getEncoder();

    static JoinResponse createSuccessJoinResponse(NodeInfoImpl requestInfo , Collection<NodeInfo> infos , String token) throws IOException {
        JoinResponse response = new JoinResponse();
        response.setType(JoinResponse.ResponseType.SUCCESS);
        for(NodeInfo i : infos)
        {
            response.addNode(((NodeInfoImpl)i).toSerializableInfo());
        }

        response.setYourInfo(requestInfo.toSerializableInfo());
        response.setToken(token);


        return response;
    }


    private static ByteBuffer[] combine(ByteBuffer header , ByteBuffer ...  buffers)
    {
        ByteBuffer[] data = new ByteBuffer[buffers.length+1];
        data[0] = header;
        for(int i=0;i<buffers.length;i++)
        {
            data[i+1] = buffers[i];
        }

        return data;
    }




    private final static ByteBuffer CLUSTER_MESSAGE_HEADER_BUFFER =
            ByteBuffer.wrap(new byte[]{MessageTypes.CLUSTER_MESSAGE})
                    .asReadOnlyBuffer();


    private final static ByteBuffer clusterMessageHeaderBuffer()
    {
        return CLUSTER_MESSAGE_HEADER_BUFFER.duplicate();
    }






    public final static AppenderSerializer CLSTR_MSG_SRLIZR =
            new AppenderSerializer(
                    ClusterProtocolConstants::clusterMessageHeaderBuffer,
                    new ObjectSerializer<>(false)
            );
}
