package shepherd.standard.cluster.node;


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

class ClusterStateTracker {



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
        private boolean timedOut;
        private final SerializableNodeInfo relatedNode;


        private DistributeAnnounce(Set<NodeInfo> allPossibleAnnouncers, Type type, SerializableNodeInfo relatedNode) {
            this.allPossibleAnnouncers = allPossibleAnnouncers;
            this.relatedNode = relatedNode;
            this.totalPossibleAnnouncers = this.allPossibleAnnouncers.size();
            this.type = type;
            this.successAnnouncers = new ConcurrentHashMap<>();
            this.failedAnnouncers = Collections.newSetFromMap(new ConcurrentHashMap<>());
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

            return allPossibleAnnouncers.isEmpty();
        }

        private boolean fail(NodeInfo info)
        {
            if(allPossibleAnnouncers.remove(info))
            {
                failedAnnouncers.add(info);
            }

            return allPossibleAnnouncers.isEmpty();
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

        public boolean isTimedOut() {
            return timedOut;
        }

        private void setTimedOut(boolean timedOut) {
            this.timedOut = timedOut;
        }
    }



    private final Map<SerializableNodeInfo , DistributeAnnounce> disconnectAnnounces;
    private final Map<SerializableNodeInfo , DistributeAnnounce> connectAnnounces;
    private BiConsumer<DistributeAnnounce , ClusterStateTracker> onAnnounceDone;
    private final StandardNode node;
    private final StandardCluster cluster;

    private final Function<SerializableNodeInfo , DistributeAnnounce> connectCreator = new Function<SerializableNodeInfo, DistributeAnnounce>() {
        @Override
        public DistributeAnnounce apply(SerializableNodeInfo info) {
            return new DistributeAnnounce(captureNodes(info) , DistributeAnnounce.Type.CONNECT, info);
        }
    };


    private final Function<SerializableNodeInfo , DistributeAnnounce> disconnectCreator = new Function<SerializableNodeInfo, DistributeAnnounce>() {
        @Override
        public DistributeAnnounce apply(SerializableNodeInfo info) {
            return new DistributeAnnounce(captureNodes(info) , DistributeAnnounce.Type.DISCONNECT, info);
        }
    };

    ClusterStateTracker(StandardNode node) {
        this.disconnectAnnounces = new ConcurrentHashMap<>();
        this.connectAnnounces = new ConcurrentHashMap<>();
        this.node = node;
        this.cluster = (StandardCluster) node.cluster();
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


    public void setOnAnnounceDone(BiConsumer<DistributeAnnounce , ClusterStateTracker> onAnnounceDone) {
        ifNull("listener is null" , onAnnounceDone);
        this.onAnnounceDone = onAnnounceDone;
    }

    public final void announceConnect(Message<ConnectAnnounce> message)
    {
        setClusterStateToSynchronizing();
        ConnectAnnounce connectAnnounce = message.data();
        DistributeAnnounce distributeAnnounce = getOrCreateConnectAnnounce(
                connectAnnounce.connectedNode()
        );

        if(distributeAnnounce.announce(message.metadata().sender() , connectAnnounce))
        {
            connectAnnounces.remove(connectAnnounce.connectedNode());
            handleAnnounce(distributeAnnounce);
        }

    }


    public final void announceDisconnect(Message<DisconnectAnnounce> message)
    {
        setClusterStateToSynchronizing();
        DisconnectAnnounce disconnectAnnounce = message.data();
        DistributeAnnounce distributeAnnounce = getOrCreateDisconnectAnnounce(
                disconnectAnnounce.disconnectedNode()
        );

        if(distributeAnnounce.announce(message.metadata().sender() , disconnectAnnounce))
        {
            disconnectAnnounces.remove(disconnectAnnounce.disconnectedNode());
            handleAnnounce(distributeAnnounce);
        }
    }


    public final void localDisconnectAnnounce(SerializableNodeInfo disconnected, boolean left)
    {
        setClusterStateToSynchronizing();
        DistributeAnnounce distributeAnnounce = getOrCreateDisconnectAnnounce(
                disconnected
        );

        if(distributeAnnounce.announce(node.info() , new DisconnectAnnounce(disconnected, left)))
        {
            disconnectAnnounces.remove(disconnected);
            handleAnnounce(distributeAnnounce);
        }
    }

    public final void localConnectAnnounce(IoChannel channel , ConnectAnnounce announce)
    {
        setClusterStateToSynchronizing();
        DistributeAnnounce distributeAnnounce = getOrCreateConnectAnnounce(
                announce.connectedNode()
        ).setChannel(channel);

        if(distributeAnnounce.announce(node.info() , announce))
        {
            connectAnnounces.remove(announce.connectedNode());
            handleAnnounce(distributeAnnounce);
        }
    }


    private final void handleAnnounce(DistributeAnnounce announce)
    {
        onAnnounceDone.accept(announce , this);

        if(disconnectAnnounces.isEmpty() && connectAnnounces.isEmpty())
        {
            //so handle it please !!!!!!!!!!!!!!
            cluster.setState(ClusterState.SYNCHRONIZED);
        }
    }


    private final void setClusterStateToSynchronizing()
    {
        if(cluster.state().is(ClusterState.SYNCHRONIZED)) {
            cluster.setState(
                    ClusterState.SYNCHRONIZING
            );
        }
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
