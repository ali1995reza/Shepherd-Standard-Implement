package shepherd.standard.cluster.node;


import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.message.Message;
import shepherd.api.message.MessageMetadata;

class MessageImpl<T> implements Message<T> {


    private T data;
    private MessageMetadataImpl metadata;

    MessageImpl(T messageData
            , int messageId
            , NodeInfo messageSender
            , byte messageType
            , long messageSendTime
            , long messageReceiveTime)
    {
        data = messageData;
        metadata = new MessageMetadataImpl(messageId ,  messageSender,  MessageMetadata.MessageType.getByCode(messageType)
                ,  messageSendTime, messageReceiveTime);
    }

    @Override
    public T data() {
        return data;
    }


    @Override
    public MessageMetadata metadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "Message{" +
                "data=" + data +
                ", metadata=" + metadata +
                '}';
    }
}
