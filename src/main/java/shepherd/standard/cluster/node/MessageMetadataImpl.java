package shepherd.standard.cluster.node;

import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.message.MessageMetadata;

class MessageMetadataImpl implements MessageMetadata {


    private final static MessageType getType(byte t)
    {
        if(t == ClusterProtocolConstants.MessageTypes.BROADCAST)
            return MessageType.BROADCAST;

        if(t == ClusterProtocolConstants.MessageTypes.MULTICAST)
            return MessageType.MULTICAST;

        if(t == ClusterProtocolConstants.MessageTypes.UNICAST)
            return MessageType.UNICAST;

        throw new IllegalArgumentException("can not find type");
    }


    private MessageType type;
    private long sendTime = -1;
    private long receiveTime = -1;
    private int id;
    private NodeInfo sender;


    MessageMetadataImpl(int messageId
            , NodeInfo messageSender
            , MessageType messageType
            , long messageSendTime
            , long messageReceiveTime)
    {
        id = messageId;
        sender = messageSender;
        this.type = messageType;
        sendTime = messageSendTime;
        receiveTime = messageReceiveTime;
    }


    @Override
    public NodeInfo sender() {
        return sender;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public MessageType type() {
        return type;
    }

    @Override
    public long sendTime() {
        return sendTime;
    }

    @Override
    public long receiveTime() {
        return receiveTime;
    }

    @Override
    public String toString() {
        return "MessageMetadata{" +
                "type=" + type +
                ", sendTime=" + sendTime +
                ", receiveTime=" + receiveTime +
                ", id=" + id +
                ", sender=" + sender +
                '}';
    }
}
