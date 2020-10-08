package shepherd.standard.cluster.node;

import shepherd.api.cluster.node.NodeInfo;

import java.nio.ByteBuffer;

final class MessageEvent implements MessageServiceEvent {

    private final ByteBuffer[] data;
    private final int serviceId;
    private final int messageId;
    private final long sendTime;
    private final long receiveTime;
    private final byte serviceHeader;
    private final NodeInfo sender;
    private final byte messageType;


    MessageEvent(ByteBuffer[] d , byte sH , int sId , int mId , long sT , long rT , byte mT , NodeInfo sndr)
    {
        data = d;
        serviceId = sId;
        messageId = mId;
        sendTime = sT;
        receiveTime = rT;
        serviceHeader = sH;
        messageType = mT;
        sender = sndr;
    }





    @Override
    public void handleBy(MessageServiceImpl service)
    {
        service.handleReceivedData(data , serviceHeader , messageId , sender , messageType , sendTime, receiveTime);
    }

    public byte serviceHeader() {
        return serviceHeader;
    }


    @Override
    public int serviceId() {
        return serviceId;
    }

    public ByteBuffer[] data() {
        return data;
    }

    public int messageId() {
        return messageId;
    }

    public long receiveTime() {
        return receiveTime;
    }

    public long sendTime() {
        return sendTime;
    }

    public NodeInfo sender() {
        return sender;
    }


}
