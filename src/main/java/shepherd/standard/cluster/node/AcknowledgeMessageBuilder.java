package shepherd.standard.cluster.node;

import java.nio.ByteBuffer;

class AcknowledgeMessageBuilder {

    private final static class AckBuilder{

        private final static byte NEW_ROUND = 1;
        private final static byte NO_NEW_ROUND = 0;



        private final byte priority;
        private int lastMessageId = 0 ;
        private long lastMessageRecvTime = -2;
        private int lastAckSent = 0 ;
        private long lastAckMessageRecvTime = -1;

        AckBuilder(byte priority)
        {
            this.priority  = priority;
        }

        void setLastMessageId(int msgId , long recvTime)
        {
            lastMessageId = msgId;
            lastMessageRecvTime = recvTime;
        }

        private boolean appendAck(ByteBuffer buffer)
        {
            if(lastMessageRecvTime<=lastAckMessageRecvTime &&
                    lastMessageId==lastAckSent)return false;


            final int currentMsgId = lastMessageId;
            final long currentMsgRecvTime = lastMessageRecvTime;

            buffer.put(priority);
            buffer.put(currentMsgId>lastAckSent?NO_NEW_ROUND:NEW_ROUND);
            buffer.putInt(currentMsgId);

            lastAckSent = currentMsgId;
            lastAckMessageRecvTime = currentMsgRecvTime;

            return true;
        }
    }



    private final AckBuilder[] ackBuilders;
    private final ByteBuffer ackBuffer;
    private final ByteBuffer[] provideAble;


    private byte maximumPrioritySet = Byte.MIN_VALUE;
    private byte minimumPrioritySet = Byte.MAX_VALUE;

    public AcknowledgeMessageBuilder(int numberOfPriorities)
    {
        ackBuilders = new AckBuilder[numberOfPriorities+1];
        for(byte i=1;i<=numberOfPriorities;i++)
        {
            ackBuilders[i] = new AckBuilder(i);
        }

        ackBuffer = ByteBuffer.allocate(numberOfPriorities*6+10);
        provideAble = new ByteBuffer[1];
        provideAble[0] = ackBuffer;
    }


    public void  setLastMessage(byte priority , int msgId , long recvTime)
    {
        ackBuilders[priority].setLastMessageId(msgId , recvTime);

        if(priority>maximumPrioritySet)
        {
            maximumPrioritySet = priority;
        }
        if(priority<minimumPrioritySet)
        {
            minimumPrioritySet = priority;
        }

    }


    public ByteBuffer[] getAckMessage()
    {

        int len = maximumPrioritySet-minimumPrioritySet;
        if(len>=0){
            ackBuffer.clear();
            ackBuffer.put(ClusterProtocolConstants.MessageTypes.ACKNOWLEDGE);
            byte count = 0;

            //set position to 3 // make space for number of acks
            ackBuffer.position(2);
            for(byte i = maximumPrioritySet;i>=minimumPrioritySet;i--)
            {
                if(ackBuilders[i].appendAck(ackBuffer))++count;
            }
            final int currentPos = ackBuffer.position();
            ackBuffer.position(1);
            ackBuffer.put(count);
            ackBuffer.position(currentPos);
            ackBuffer.flip();
            maximumPrioritySet = Byte.MIN_VALUE;
            minimumPrioritySet = Byte.MAX_VALUE;
            return provideAble;
        }

        return null;
    }

}
