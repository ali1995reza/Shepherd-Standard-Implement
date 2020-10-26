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




    private static final Base64.Encoder encoder = Base64.getEncoder();

    static JoinResponse createSuccessJoinResponse(StandardNodeInfo requestInfo , Collection<NodeInfo> infos , String token) throws IOException {
        JoinResponse response = new JoinResponse();
        response.setType(JoinResponse.ResponseType.SUCCESS);
        for(NodeInfo i : infos)
        {
            response.addNode(((StandardNodeInfo)i).toSerializableInfo());
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
