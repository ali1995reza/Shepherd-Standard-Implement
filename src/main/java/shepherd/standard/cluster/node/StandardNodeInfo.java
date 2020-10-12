package shepherd.standard.cluster.node;

import shepherd.api.cluster.node.NodeAddress;
import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.cluster.node.NodeState;
import shepherd.api.cluster.node.NodeStatistics;
import shepherd.api.logger.Logger;
import shepherd.api.logger.LoggerFactory;
import shepherd.standard.cluster.node.clusterlevelmessage.SerializableNodeInfo;
import shepherd.standard.datachannel.IoChannel;
import shepherd.utils.concurrency.lock.PriorityLock;
import shepherd.utils.concurrency.waitobject.IWaitObjectNotifier;
import shepherd.utils.concurrency.waitobject.SyncWaitObjectNotifier;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Timer;

final class StandardNodeInfo implements NodeInfo {

    private final static class StatisticsImpl implements NodeStatistics {

        private final StandardNodeInfo info;

        private StatisticsImpl(StandardNodeInfo info) {
            this.info = info;
        }

        @Override
        public long totalSentBytes() {
            return info.ioChannel.totalSentBytes();
        }

        @Override
        public long totalReceivedBytes() {
            return info.ioChannel.totalReceivedBytes();
        }

        @Override
        public long totalSentPackets() {
            return info.ioChannel.totalSentPackets();
        }

        @Override
        public long totalReceivedPackets() {
            return info.ioChannel.totalReceivedPackets();
        }
    }

    private final static class ZeroStatistics implements NodeStatistics{


        @Override
        public long totalSentBytes() {
            return 0;
        }

        @Override
        public long totalReceivedBytes() {
            return 0;
        }

        @Override
        public long totalSentPackets() {
            return 0;
        }

        @Override
        public long totalReceivedPackets() {
            return 0;
        }
    }


    private final Logger logger;


    private NodeState state = NodeState.UNKNOWN;
    private final Object _statListenerSync = new Object();
    private long joinTime;
    private int id;
    private NodeAddress<SocketAddress> address;
    private boolean isLeader;
    private final IoChannel ioChannel;
    private final IWaitObjectNotifier<AcknowledgeImpl>[] acknowledgeNotifiers;
    private boolean isCurrentNode;
    private ClusterEventImpl clusterEvent;
    private final Timer sharedTimer;
    private final AcknowledgeMessageBuilder acknowledgeMessageBuilder =
            new AcknowledgeMessageBuilder(10);

    private final PriorityLock lock;


    private String hashId;


    private final NodeStatistics statistics;

    public StandardNodeInfo(int id ,
                            long joinTime ,
                            NodeAddress<SocketAddress> address ,
                            boolean isLeader  ,
                            boolean isCurrentNode,
                            StandardNode node , IoChannel ioChannel)
    {
        this.id = id;
        this.joinTime = joinTime;
        this.address = address;
        this.isLeader = isLeader;
        this.isCurrentNode = isCurrentNode;
        this.clusterEvent = (ClusterEventImpl)node.cluster().clusterEvent();
        this.sharedTimer = node.sharedTimer();
        acknowledgeNotifiers = new IWaitObjectNotifier[11];
        for(int i=1;i<acknowledgeNotifiers.length;i++)
            acknowledgeNotifiers[i] = new SyncWaitObjectNotifier();

        this.ioChannel = ioChannel;

        lock = node.priorityLock();

        statistics = isCurrentNode?new ZeroStatistics():
                new StatisticsImpl(this);

        logger = LoggerFactory.factory().getLogger(this);
    }


    public StandardNodeInfo setHashId(String hashId) {
        this.hashId = hashId;
        return this;
    }

    public String hashId() {
        return hashId;
    }

    void notifyAcknowledges(byte priority , int id , int code)
    {
        acknowledgeNotifiers[priority].notifyObjects(id , code);
    }



    void setLastMessageId(byte priority , int msgId , long recvTime)
    {

        acknowledgeMessageBuilder.setLastMessage(priority , msgId, recvTime);
    }


    void sendAck()
    {
        ByteBuffer[] ack = acknowledgeMessageBuilder.getAckMessage();

        if(ack==null) return;

        lock.lockMaximumPriority();
        try {
            ioChannel.send(ack , MAXIMUM_PRIORITY);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            lock.unlock();
        }

    }



    public void setState(NodeState state) {

        synchronized (_statListenerSync) {

            NodeState lastState = null;
            lock.lockMaximumPriority();
            try {
                lastState = this.state;
                this.state = state;

            }finally {

                lock.unlock();
            }

            clusterEvent.notifyNodeStateChanged(this , lastState , this.state);
        }
    }

    @Override
    public int id() {
        return id;
    }




    @Override
    public NodeAddress<SocketAddress> address() {
        return address;
    }

    @Override
    public NodeState state() {

        return state;
    }


    private final static byte MAXIMUM_PRIORITY = 10;
    final boolean send(ByteBuffer header , ByteBuffer[] data  , AcknowledgeImpl ack , int msgId , byte priority , int serviceId)
    {

        if(state==NodeState.CLUSTER_CONNECTED ||
                (state==NodeState.LEAVING && (serviceId==0 || serviceId==1))) {
            try {
                acknowledgeNotifiers[priority].waitForNotify(ack, msgId);
            }catch (Throwable e)
            {
                logger.warning(e);
                ack.notifyObject(0);
                return false;
            }
            try {
                ioChannel.send(header, data , priority);
            }catch (Throwable e)
            {
                //so must handleBy !
                ack.notifyObject(0);
                logger.warning("packet didn't send" , e);
                return false;
            }
            return true;
        }else
        {
            logger.warning("packet didn't send cause state is {}" , state);
            ack.notifyObject(0);
            return false;
        }


    }

    final boolean send(ByteBuffer header , ByteBuffer[] data , byte priority , int serviceId)
    {
        try {

            if(state==NodeState.CLUSTER_CONNECTED ||
                    (state==NodeState.LEAVING&&(serviceId==1 || serviceId==0))) {


                ioChannel.send(header, data, priority);
                return true;
            }else {
                logger.warning("packet didn't send cause state is {}" , state);
                return false;
            }
        } catch (Throwable e) {
            logger.warning("packet didn't send" , e);
            return false;
        }
    }





    @Override
    public boolean isCurrentNode() {
        return false;
    }

    public void setAddress(NodeAddress<SocketAddress> address) {
        this.address = address;
    }

    @Override
    public boolean isLeader() {
        return isLeader;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setAsLeader()
    {
        isLeader = true;
        clusterEvent.notifyLeaderChanged(this);
    }

    @Override
    public long joinTime() {
        return joinTime;
    }

    @Override
    public long version() {
        return 0;
    }

    @Override
    public NodeStatistics statistics() {
        return statistics;
    }


    public SerializableNodeInfo toSerializableInfo()
    {
        return new SerializableNodeInfo()
                .setAddress(address.address())
                .setId(id)
                .setVersion(version())
                .setJoinTime(joinTime)
                .setLeader(isLeader)
                .setHashId(hashId);
    }

    public void setJoinTime(long joinTime) {
        this.joinTime = joinTime;
    }


    @Override
    public String toString() {
        String str = "[ID : "+id+", ";
        str += "Address : "+address+", ";
        str += "Join Time : "+joinTime+", ";
        str += "State : "+state+", ";
        str += "CurrentNode : "+isCurrentNode+", ";
        str += "Hash-ID : "+hashId+", ";
        str += "Leader : "+ isLeader +"]";
        return str;
    }



    final static StandardNodeInfo parseSerializedInfo(SerializableNodeInfo info , StandardNode node , IoChannel ioChannel)
    {
        return new StandardNodeInfo(
                info.id() ,
                info.joinTime() ,
                new NodeSocketAddress(info.address()) ,
                info.isLeader() ,
                false ,
                node , ioChannel
        ).setHashId(info.hashId());
    }
}
