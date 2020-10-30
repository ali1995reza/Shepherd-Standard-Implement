package shepherd.standard.cluster.node;


import shepherd.api.cluster.Cluster;
import shepherd.api.cluster.node.Node;
import shepherd.standard.cluster.node.clusterlevelmessage.*;
import shepherd.standard.datachannel.IoChannel;
import shepherd.api.cluster.ClusterState;
import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.message.Message;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static shepherd.standard.assertion.Assertion.*;

class AnnouncesStateTracker {



    public final static class DistributeAnnounce{

        public enum Type{
            DISCONNECT , CONNECT ;

            public boolean is(Type other)
            {
                return this == other;
            }
        }


        private final Set<NodeInfo> allPossibleAnnouncers;
        private final Map<NodeInfo , Announce> successAnnouncers;
        private final Set<NodeInfo> failedAnnouncers;
        private final int totalPossibleAnnouncers;
        private final Type type;
        private IoChannel channel;
        private final SerializableNodeInfo relatedNode;
        private final long startTime;
        private long lastActivityTime;
        private long localDetectTime;
        private final Cluster cluster;

        private DistributeAnnounce(Set<NodeInfo> allPossibleAnnouncers, Type type, SerializableNodeInfo relatedNode , Cluster c) {
            this.allPossibleAnnouncers = allPossibleAnnouncers;
            this.relatedNode = relatedNode;
            this.totalPossibleAnnouncers = this.allPossibleAnnouncers.size();
            this.type = type;
            this.successAnnouncers = new ConcurrentHashMap<>();
            this.failedAnnouncers = Collections.newSetFromMap(new ConcurrentHashMap<>());
            this.cluster = c;
            startTime = cluster.clusterTime();
            lastActivityTime = startTime;
        }


        private DistributeAnnounce setChannel(IoChannel channel) {
            this.channel = channel;
            return this;
        }

        private boolean announce(NodeInfo info , Announce announce)
        {
            ifFalse("announcer can not do it" ,
                    allPossibleAnnouncers.remove(info));

            ifFalse("announce and type not the same" ,
                    (type.is(Type.DISCONNECT) && announce instanceof DisconnectAnnounce) ||
                            (type.is(Type.CONNECT) && announce instanceof ConnectAnnounce));

            successAnnouncers.put(info , announce);

            lastActivityTime = cluster.clusterTime();

            return allPossibleAnnouncers.isEmpty();
        }


        private boolean localAnnounce(NodeInfo info , Announce announce)
        {
            boolean b = announce(info , announce);
            localDetectTime = cluster.clusterTime();
            return b;
        }

        private boolean fail(NodeInfo info)
        {
            if(allPossibleAnnouncers.remove(info))
            {
                failedAnnouncers.add(info);
            }

            return allPossibleAnnouncers.isEmpty();
        }

        /**
         *
         * @param info the information that must removed from announce
         * @return -1 if can remove whole announce , 0 if nothing and 1 if ready to response
         */
        private int remove(NodeInfo info)
        {

            boolean b = failedAnnouncers.remove(info)
                ||successAnnouncers.remove(info)!=null
                    ||allPossibleAnnouncers.remove(info);

            if(!b)return 0;

            lastActivityTime = cluster.clusterTime();

            if(successAnnouncers.size()
                    +failedAnnouncers.size()<=0)
            {
                return -1;
            }

            return allPossibleAnnouncers.size()==0?1:0;
        }

        public Set<NodeInfo> failedAnnouncers() {
            return failedAnnouncers;
        }

        public Map<NodeInfo , Announce> announces() {
            return successAnnouncers;
        }

        public int totalPossibleAnnouncers() {
            return totalPossibleAnnouncers;
        }

        public IoChannel channel() {
            return channel;
        }

        public Type type() {
            return type;
        }

        public SerializableNodeInfo relatedNode() {
            return relatedNode;
        }

        public void setLocalDetectTime(long localDetectTime) {
            this.localDetectTime = localDetectTime;
        }
    }



    private final Map<SerializableNodeInfo , DistributeAnnounce> disconnectAnnounces;
    private final Map<SerializableNodeInfo , DistributeAnnounce> connectAnnounces;
    private final StandardNode node;

    private final Function<SerializableNodeInfo , DistributeAnnounce> connectCreator = new Function<SerializableNodeInfo, DistributeAnnounce>() {
        @Override
        public DistributeAnnounce apply(SerializableNodeInfo info) {
            return new DistributeAnnounce(captureNodes(info) , DistributeAnnounce.Type.CONNECT, info, node.cluster());
        }
    };


    private final Function<SerializableNodeInfo , DistributeAnnounce> disconnectCreator = new Function<SerializableNodeInfo, DistributeAnnounce>() {
        @Override
        public DistributeAnnounce apply(SerializableNodeInfo info) {
            return new DistributeAnnounce(captureNodes(info) , DistributeAnnounce.Type.DISCONNECT, info ,node.cluster());
        }
    };

    AnnouncesStateTracker(StandardNode node) {
        this.disconnectAnnounces = new ConcurrentHashMap<>();
        this.connectAnnounces = new ConcurrentHashMap<>();
        this.node = node;
    }


    private final DistributeAnnounce getOrCreateAnnounce(Map announceMap , SerializableNodeInfo info)
    {
        if(announceMap == disconnectAnnounces)
        {
            return disconnectAnnounces.computeIfAbsent(info , disconnectCreator);
        }else if(announceMap == connectAnnounces)
        {
            return connectAnnounces.computeIfAbsent(info , connectCreator);
        }else
        {
            ifTrue("invalid map" , true);
        }

        return null;
    }

    private final DistributeAnnounce getOrCreateConnectAnnounce(SerializableNodeInfo nodeInfo)
    {
        return getOrCreateAnnounce(connectAnnounces , nodeInfo);
    }

    private final DistributeAnnounce getOrCreateDisconnectAnnounce(SerializableNodeInfo nodeInfo)
    {
        return getOrCreateAnnounce(disconnectAnnounces , nodeInfo);
    }



    public final DistributeAnnounce announceConnect(Message<ConnectAnnounce> message)
    {
        ConnectAnnounce connectAnnounce = message.data();
        DistributeAnnounce distributeAnnounce = getOrCreateConnectAnnounce(
                connectAnnounce.connectedNode()
        );

        if(distributeAnnounce.announce(message.metadata().sender() , connectAnnounce))
        {
            return connectAnnounces.remove(connectAnnounce.connectedNode());
        }

        return null;

    }


    public final DistributeAnnounce announceDisconnect(Message<DisconnectAnnounce> message)
    {
        DisconnectAnnounce disconnectAnnounce = message.data();
        DistributeAnnounce distributeAnnounce = getOrCreateDisconnectAnnounce(
                disconnectAnnounce.disconnectedNode()
        );

        if(distributeAnnounce.announce(message.metadata().sender() , disconnectAnnounce))
        {
            return disconnectAnnounces.remove(disconnectAnnounce.disconnectedNode());

        }

        return null;
    }


    public final DistributeAnnounce localDisconnectAnnounce(SerializableNodeInfo disconnected, boolean left)
    {

        DistributeAnnounce distributeAnnounce = getOrCreateDisconnectAnnounce(
                disconnected
        );

        if(distributeAnnounce.localAnnounce(node.info() , new DisconnectAnnounce(disconnected, left)))
        {
            return disconnectAnnounces.remove(disconnected);
        }

        return null;
    }

    public final DistributeAnnounce localConnectAnnounce(IoChannel channel , ConnectAnnounce announce)
    {
        DistributeAnnounce distributeAnnounce = getOrCreateConnectAnnounce(
                announce.connectedNode()
        ).setChannel(channel);

        if(distributeAnnounce.localAnnounce(node.info() , announce))
        {
            return connectAnnounces.remove(announce.connectedNode());
        }

        return null;
    }

    public final List<DistributeAnnounce> failDisconnectAnnounces(NodeInfo info)
    {
        List<DistributeAnnounce> doneAnnounces = new ArrayList<>();
        for(DistributeAnnounce announce:disconnectAnnounces.values())
        {
            if(announce.fail(info))
            {
                doneAnnounces.add(announce);
            }
        }

        for(DistributeAnnounce announce:doneAnnounces)
        {
            disconnectAnnounces.remove(announce.relatedNode);
        }

        return doneAnnounces;
    }

    public final List<DistributeAnnounce> failConnectAnnounces(NodeInfo info)
    {
        List<DistributeAnnounce> doneAnnounces = new ArrayList<>();
        for(DistributeAnnounce announce:connectAnnounces.values())
        {
            if(announce.fail(info))
            {
                doneAnnounces.add(announce);
            }
        }

        for(DistributeAnnounce announce:doneAnnounces)
        {
            connectAnnounces.remove(announce.relatedNode);
        }

        return doneAnnounces;
    }

    public final List<DistributeAnnounce> removeFromDisconnectAnnounces(NodeInfo info)
    {
        List<DistributeAnnounce> done = new ArrayList<>();
        List<DistributeAnnounce> remove = new ArrayList<>();
        for(DistributeAnnounce announce:disconnectAnnounces.values())
        {
            int code = announce.remove(info);

            if(code==0) continue;

            remove.add(announce);

            if(code>0) done.add(announce);
        }

        for(DistributeAnnounce announce:remove)
        {
            disconnectAnnounces.remove(announce.relatedNode);
        }

        return done;
    }

    public final List<DistributeAnnounce> timeOutAnnounces(long l)
    {
        if(disconnectAnnounces.size()+connectAnnounces.size()<=0)
            return Collections.EMPTY_LIST;

        List<DistributeAnnounce> announces = new ArrayList<>();

        for(DistributeAnnounce announce:disconnectAnnounces.values())
        {
            if(node.cluster().clusterTime()-announce.lastActivityTime>=l)
                announces.add(announce);
        }

        for(DistributeAnnounce announce:connectAnnounces.values())
        {
            if(node.cluster().clusterTime()-announce.lastActivityTime>=l)
                announces.add(announce);
        }

        return announces;
    }



    private final Set<NodeInfo> captureNodes(SerializableNodeInfo target)
    {
        Set<NodeInfo> captured = new HashSet<>();

        for(NodeInfo info:node.cluster().schema().nodes().values())
        {
            if(target.id() == info.id()) continue;

            captured.add(info);

        }

        return captured;
    }

    int numberOfRemainingAnnounces()
    {
        return disconnectAnnounces.size()+connectAnnounces.size();
    }

    boolean hasRemainingAnnounces()
    {
        return numberOfRemainingAnnounces()>0;
    }

}
