package shepherd.standard.cluster.node;


import shepherd.api.cluster.Cluster;
import shepherd.api.cluster.ClusterState;
import shepherd.api.cluster.node.Node;
import shepherd.api.cluster.node.NodeAddress;
import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.cluster.node.NodeState;
import shepherd.api.config.ConfigurationChangeResult;
import shepherd.api.config.ConfigurationKey;
import shepherd.api.config.IConfiguration;
import shepherd.api.logger.Logger;
import shepherd.api.logger.LoggerFactory;
import shepherd.api.message.MessageServiceManager;
import shepherd.api.message.exceptions.SerializeException;
import shepherd.standard.assertion.Assertion;
import shepherd.standard.cluster.node.clusterlevelmessage.*;
import shepherd.standard.config.ConfigChangeResult;
import shepherd.standard.config.Configuration;
import shepherd.standard.datachannel.IoChannel;
import shepherd.standard.datachannel.IoChannelCenter;
import shepherd.standard.datachannel.IoChannelEventListener;
import shepherd.standard.datachannel.standard.StandardIoChannelCenter;
import shepherd.standard.utils.Hash;
import shepherd.utils.buffer.ByteBufferArray;
import shepherd.utils.buffer.Utils;
import shepherd.utils.concurrency.lock.PriorityLock;
import shepherd.utils.concurrency.lock.ReentrantPriorityLock;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;

import static shepherd.standard.assertion.Assertion.*;
import static shepherd.standard.cluster.node.ClusterProtocolConstants.CLSTR_MSG_SRLIZR;
import static shepherd.standard.cluster.node.ClusterProtocolConstants.MessageTypes;


public class StandardNode implements Node<SocketAddress> {

    private final static class JoinEventListener  implements IoChannelEventListener{




        private final static int INITIAL = 0;
        private final static int CONNECTED_TO_LEADER = 1;
        private final static int SEND_JOIN_REQUEST = 2;
        private final static int JOIN_RESPONSE_RECEIVED = 3;
        private final static int DISCOVER_NODES = 4;
        private final static int TIME_SYNC = 5;
        private final static int READY = 6;



        private final StandardNode node;
        private final Logger logger;
        private Throwable joinError;
        private final CountDownLatch joinLatch = new CountDownLatch(1);
        private IoChannel leaderChannel;
        private int state = INITIAL;
        private int numberOfNodes = -1;

        private JoinEventListener(StandardNode node) {
            this.node = node;
            this.logger = node.logger;
        }

        private final Object deserialize(ByteBuffer[] data) throws SerializeException {
            return CLSTR_MSG_SRLIZR.deserialize(data);
        }

        private final ByteBuffer[] serialize(Object object) throws SerializeException {
            return CLSTR_MSG_SRLIZR.serialize(object);
        }

        public void join(NodeAddress<SocketAddress> nodeAddress , String password)
        {
            logger.information("set join event listener");
            node.ioChannelCenter.setDataChannelEventListener(this);
            try {
                logger.information("joining to cluster , address : {}" , nodeAddress.address());
                doJoin(nodeAddress , password);
            } catch (Exception e) {
                logger.error(e);
                throw new IllegalStateException(e);
            }
            logger.information("waiting to joining process end");
            waitToJoinProcessEnd();
            if(joinError!=null) {
                logger.error("join process ended with error");
                throw new IllegalStateException(joinError);
            }else
            {
                logger.information("join process ended successfully");
            }
        }

        private final void waitToJoinProcessEnd()
        {
            try {
                joinLatch.await();
            } catch (InterruptedException e) {
                logger.warning(e);
                while (joinLatch.getCount()>0);
            }
        }


        private final void doJoin(NodeAddress<SocketAddress> address , String password) throws Exception {
            logger.information("connecting to {}" , address.address());
            leaderChannel = node.ioChannelCenter.connect(address.address());
            ++state;
            SocketAddress currentNodeAddress =
                    node.configurations().get(NodeConfigurations.NODE_ADDRESS);
            logger.information("current node address is {}",currentNodeAddress);
            ifNull("current node address not set" , currentNodeAddress);
            JoinRequest joinReq = new JoinRequest(currentNodeAddress , password);
            ++state;
            logger.information("sending join request");
            leaderChannel.send(serialize(joinReq) , node.MAXIMUM_PRIORITY);

        }



        @Override
        public synchronized void onChannelDisconnected(IoChannel ioChannel) {
            joinError  = new IOException("a connection disconnected during join time");
            logger.warning(joinError);
            joinLatch.countDown();
        }

        @Override
        public synchronized void onNewChannelConnected(IoChannel ioChannel) {
            joinError = new IOException("a new connection connected during join time");
            logger.warning(joinError);
            joinLatch.countDown();
        }

        @Override
        public synchronized void onDataReceived(IoChannel ioChannel, ByteBuffer[] data, byte priority) {
            try {
                handleMessage(ioChannel, data, priority);
            }catch (Throwable e)
            {
                joinError = e;
                logger.error(e);
                joinLatch.countDown();
            }
        }

        @Override
        public synchronized void onReadRoundEnd(IoChannel ioChannel) {
        }

        @Override
        public synchronized void onWriteRoundEnd(IoChannel ioChannel) {

        }


        private final void handleMessage(IoChannel ioChannel, ByteBuffer[] data, byte priority) throws Throwable
        {

            byte type = Utils.getByte(data);

            if (type == MessageTypes.CLUSTER_MESSAGE) {
                Object message = deserialize(data);

                if(message instanceof JoinResponse)
                {

                    JoinResponse joinResponse = (JoinResponse) message;

                    ifFalse("JoinResponse Received in bad state" , state==SEND_JOIN_REQUEST);
                    ifFalse("JoinResponse Received from bad io-channel" , ioChannel==leaderChannel);
                    ifFalse("Join Failed - Reason : "+joinResponse.type() ,
                            joinResponse.type().is(JoinResponse.ResponseType.SUCCESS));

                    ++state;
                    ++state;

                    numberOfNodes = joinResponse.nodes().size();

                    node.currentNodeInfo
                            .setId(joinResponse.yourInfo().id());
                    node.currentNodeInfo
                            .setJoinTime(joinResponse.yourInfo().joinTime());


                    node.currentNodeInfo
                            .setState(NodeState.SYNCHRONIZING);

                    node.cluster.setState(
                            ClusterState.SYNCHRONIZING
                    );

                    for(SerializableNodeInfo info:joinResponse.nodes())
                    {
                        if(info.isLeader())
                        {
                            ioChannel.attach(info);
                            ioChannel.send(
                                    serialize(joinResponse.toConnectRequest()) ,
                                    node.MAXIMUM_PRIORITY
                            );
                        }else
                        {
                            IoChannel newChannel = node.ioChannelCenter.connect(
                                    info.address()
                            );
                            newChannel.attach(info);
                            newChannel.send(
                                    serialize(joinResponse.toConnectRequest()) ,
                                    node.MAXIMUM_PRIORITY
                            );
                        }
                    }

                }else if(message instanceof ConnectResponse)
                {

                    ifFalse("bad state to received this message" , state==DISCOVER_NODES);

                    ConnectResponse response = (ConnectResponse) message;

                    ifNull("attachment is null" , ioChannel.attachment());
                    ifTrue("connect response received multiple time" ,
                            ioChannel.attachment() instanceof StandardNodeInfo);
                    ifFalse("connect response not success from node with ID["+((SerializableNodeInfo)ioChannel.attachment()).id()+"]" ,
                            response.isSuccess());
                    ifFalse("connect response not match with connect request" ,
                            response.info().equals(ioChannel.attachment()));

                    StandardNodeInfo nodeInfo =
                            StandardNodeInfo.parseSerializedInfo(response.info() , node , ioChannel);
                    ioChannel.attach(nodeInfo);
                    node.nodesList.addNode(
                            nodeInfo
                    );

                    nodeInfo.setState(NodeState.CONNECTING);
                    nodeInfo.setState(NodeState.SYNCHRONIZING);
                    nodeInfo.setState(NodeState.CONNECTED);

                    ifTrue("more than possible node request to handle connect response" ,
                            --numberOfNodes<0);

                    node.hashCalculator.addHash(
                            nodeInfo.id() ,
                            response.hashId()
                    );

                    if(numberOfNodes==0)
                    {
                        String hashId = node.hashCalculator.calculateHash();
                        logger.information("hash id is {}" , hashId);
                        node.hashCalculator.refresh();

                        node.currentNodeInfo.setHashId(hashId);

                        leaderChannel.send(
                                serialize(TimeSync.now()),
                                node.MAXIMUM_PRIORITY
                        );
                        ++state;
                    }


                }else if(message instanceof TimeSync){

                    ifFalse("bad state" , state==TIME_SYNC);

                    TimeSync timeSync = (TimeSync) message;

                    long time = ManagementFactory.getRuntimeMXBean().getUptime();
                    time = (time-timeSync.sendTime())/2;
                    time = timeSync.clusterTime()+time;
                    node.cluster.setClusterPointTime(
                            time
                    );

                    logger.information("cluster time synchronized , Time is : {}" , node.cluster.clusterTime());

                    node.ioChannelCenter
                            .setDataChannelEventListener(
                                    node.ioChannelEventListener
                            );

                    node.nodesList.addNode(node.currentNodeInfo);

                    ReadyAnnounce announce =
                            ReadyAnnounce.withHashId(node.currentNodeInfo.hashId());

                    for(IoChannel channel:node.ioChannelCenter.connectedChannels())
                    {

                        channel.send(serialize(announce) ,
                                node.MAXIMUM_PRIORITY
                        );

                        StandardNodeInfo info = channel.attachment();
                        info.setState(NodeState.CLUSTER_CONNECTED);

                    }


                    node.currentNodeInfo.setState(NodeState.CLUSTER_CONNECTED);


                    ++state;
                    node.cluster.setState(ClusterState.SYNCHRONIZED);
                    joinLatch.countDown();

                }else
                {
                    ifTrue("un specific message at this time" , true);
                }
            }else
            {
                ifTrue("un specific message at this time" , true);
            }
        }
    }







    //---------------------------class fields
    private final IoChannelEventListener ioChannelEventListener = new IoChannelEventListener() {
        @Override
        public void onChannelDisconnected(IoChannel ioChannel) {
            clusterController.put(ClusterLevelEvent.disconnectEvent(ioChannel));
        }

        @Override
        public void onNewChannelConnected(IoChannel ioChannel) {
            clusterController.put(ClusterLevelEvent.connectEvent(ioChannel));
        }


        @Override
        public void onDataReceived(IoChannel ioChannel, ByteBuffer[] data , byte priority) {
            try{
                handleReceivedData(priority , ioChannel , data);
            }catch (Throwable e)
            {
                logger.error("An error occurs when handling received data" , e);
                ioChannel.closeNow();
            }
        }

        @Override
        public void onReadRoundEnd(IoChannel ioChannel) {

            try {
                StandardNodeInfo nodeInfo = ioChannel.attachment();
                if (nodeInfo != null) nodeInfo.sendAck();
            }catch (Throwable e)
            {
                logger.error("An error occurs when sending ack" , e);
                ioChannel.closeNow();
            }
        }

        @Override
        public void onWriteRoundEnd(IoChannel ioChannel) {
            //todo nothing here !
        }

        @Override
        public String toString() {
            return "MAIN EVENT HANDLER";
        }
    };


    private ClusterController clusterController;
    private IoChannelCenter ioChannelCenter;
    private StandardNodeInfo currentNodeInfo;
    private StandardCluster cluster;
    private NodesListManager nodesList;
    private AcknowledgeHandler acknowledgeHandler;
    private final Object _sync = new Object();
    private final MessageServiceManagerImpl serviceManager;
    private final Configuration nodeConfigs ;
    private boolean dataChannelRun = false;
    private boolean disposed = false;
    private boolean leaving  = false;
    private boolean connectedToCluster = false;
    private final Timer sharedTimer = new Timer(true);
    private final PriorityLock priorityLock;

    private final byte MAXIMUM_PRIORITY = 10;

    private final Logger logger;


    private final JoinHashCalculator hashCalculator;


    //----------------------------------------------------public methods



    public StandardNode() {

        nodesList = new NodesListManager();

        priorityLock = new ReentrantPriorityLock(11);

        logger = LoggerFactory.factory().getLogger(this);

        initCurrentCluster();

        nodeConfigs = new Configuration("NodeConfig");
        this.ioChannelCenter = new StandardIoChannelCenter(configurations(), new IoChannelEventListener() {
            @Override
            public void onChannelDisconnected(IoChannel ioChannel) {

            }

            @Override
            public void onNewChannelConnected(IoChannel ioChannel) {

            }

            @Override
            public void onDataReceived(IoChannel ioChannel, ByteBuffer[] message, byte priority) {

            }

            @Override
            public void onReadRoundEnd(IoChannel ioChannel) {

            }

            @Override
            public void onWriteRoundEnd(IoChannel ioChannel) {

            }

            @Override
            public String toString() {
                return "EMPTY";
            }
        });
        acknowledgeHandler = new AcknowledgeHandler();
        serviceManager = new MessageServiceManagerImpl(this);


        nodeConfigs.defineConfiguration(
                NodeConfigurations.NODE_ADDRESS ,
                null ,
                this::approveConfigChange
        );

        hashCalculator = new JoinHashCalculator();
    }





    @Override
    public NodeInfo info() {
        assertIfDisposed();
        return currentNodeInfo;
    }


    @Override
    public IConfiguration configurations() {
        assertIfDisposed();
        return nodeConfigs.asUndefinableConfiguration();
    }

    @Override
    public MessageServiceManager messageServiceManager() {
        assertIfDisposed();
        return serviceManager;
    }


    @Override
    public void joinCluster(NodeAddress<SocketAddress> address) throws Exception {

        synchronized (_sync) {

            assertIfStateNotValidForJoinOrCreate();
            initCurrentNodeInfo();
            runDataChannel();
            runClusterEventHandler();
            cluster.clusterEventImpl().start();


            try {

                logger.information("Connecting to bootstrap address : {}" , address.address());
                doJoin(address , "somePAsswrd");
                runMessageServiceEventHandler();
                acknowledgeHandler.start();
                connectedToCluster  = true;

            } catch (Throwable e) {
                dispose();
                throw new IOException(e);
            }
        }

    }

    @Override
    public void createCluster() {

        synchronized (_sync) {

            assertIfStateNotValidForJoinOrCreate();

            try {
                doCreateCluster();
                connectedToCluster = true;
            } catch (Throwable e) {
                //handle it please !
                dispose();
                throw new IllegalStateException(e);
            }
        }

    }

    @Override
    public Cluster cluster() {
        return cluster;
    }

    @Override
    public void leave() {
        synchronized (_sync)
        {
            //leave cluster please !
            try {
                doLeave();
            } catch (Throwable e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private void doJoin(NodeAddress<SocketAddress> address , String password)
    {
        new JoinEventListener(this)
                .join(
                        address , password
                );
    }



    private void doLeave() throws Throwable
    {

        assertIfStateNotValidForLeave();


        serviceManager.messageDispatcherBuilder().runSynchronized(
                ()->{leaving = true;}
        );

        clusterController.handleLeave();

        dispose();



    }










    //-------------------------------------------package-private methods


    final NodesListManager nodesList()
    {
        return nodesList;
    }

    final int clusterNextId()
    {
        return cluster.nextID();
    }

    final StandardNodeInfo currentClusterLeader()
    {
        return cluster.leader();
    }

    final void releaseAllEnqueuedAcknowledgesListener(StandardNodeInfo info)
    {
        for(byte i=1;i<11;i++) {
            acknowledgeHandler.handleAcknowledge(
                    new AcknowledgeResponse(Integer.MAX_VALUE, 0, info , i)
            );

            acknowledgeHandler.handleAcknowledge(
                    new AcknowledgeResponse(Integer.MAX_VALUE, 0, info , i)
            );
        }
    }

    final void dispose()
    {
        synchronized (_sync) {

            if(disposed)
            {
                logger.warning("node already disposed");
                return;
            }


            logger.information("disposing node {} "  , currentNodeInfo);

            serviceManager.messageDispatcherBuilder().runSynchronized(
                    () -> {
                        disposed = true;
                    }
            );


            clearAndStopThreadsAndServices();

            logger.information("node disposed");
        }

    }




    //-------------------------------------------class-private methods

    private void clearAndStopThreadsAndServices()
    {
        if (ioChannelCenter != null)
            runSafe(ioChannelCenter::stop , "IoChannelCenter::stop()");
        if (clusterController != null)
            runSafe(clusterController::stop , "ClusterController::stop()");
        if (sharedTimer != null)
            runSafe(sharedTimer::cancel , "Timer::cancel()");

        if (serviceManager != null)
            runSafe(serviceManager::stopAllServices , "MessageServiceManager::stopAllServices()");
        if (acknowledgeHandler != null)
            runSafe(acknowledgeHandler::stop , "AcknowledgeHandler::stop()");

        if (cluster != null)
            runSafe(cluster::stopAllServices , "ClusterImpl::stopAllServices()");
    }


    private void runDataChannel() throws IOException {
        if(dataChannelRun)
            return;

        dataChannelRun = true;
        logger.information("Running data channel server [Type:"+ ioChannelCenter.getClass()+"] ");
        try {
            ioChannelCenter.setDataChannelEventListener(ioChannelEventListener);
            ioChannelCenter.start(currentNodeInfo.address().address());
        }catch (IOException e)
        {
            logger.error("An exception occurs when running data channel server\n"+e.getMessage());
            throw e;
        }
    }


    private void assertIfDisposed()
    {
        Assertion.ifTrue("node disposed" , disposed);
    }



    private void runClusterEventHandler()
    {
        if(clusterController ==null)
        {
            logger.information("Running ClusterLevel-Event-Handler");
            clusterController = new ClusterController(this);
            clusterController.start();
        }
    }


    private void runMessageServiceEventHandler()
    {
        logger.information("Running Message-Event-Handler");
        serviceManager.messageServiceEventHandler().start();
    }


    private void initCurrentNodeInfo()
    {


        if(currentNodeInfo==null){
            logger.information("getting address config");
            SocketAddress address = nodeConfigs.get(NodeConfigurations.NODE_ADDRESS);
            ifNull("node address not set yet" , address);
            logger.information("Init current node information");
            currentNodeInfo = new StandardNodeInfo(-1 , -1 ,
                    new NodeSocketAddress(address), false , true , this , null);
        }
    }



    private ConfigurationChangeResult approveConfigChange(IConfiguration parent , ConfigurationKey config , Object oldVal , Object newVal)
    {
        if(config == NodeConfigurations.NODE_ADDRESS)
        {
            if(newVal instanceof SocketAddress)
            {
                //todo use config
                return ConfigChangeResult.newSuccessResult(newVal);
            }else if(newVal instanceof Integer)
            {
                int i = (int)newVal;
                if(i>0 && i<65536)
                {
                    //port range !
                    return ConfigChangeResult.newSuccessResult(new InetSocketAddress(i));
                }
            }

            return ConfigChangeResult.newFailResult("Can not change config");
        }
        return ConfigChangeResult.newFailResult("Can not change config");
    }

    private void initCurrentCluster()
    {

        if(cluster==null)
        {
            cluster = new StandardCluster(nodesList);
        }
    }




    private final void handleReceivedData(byte priority , IoChannel ioChannel , ByteBuffer[] data )
    {
        byte messageType = Utils.getByte(data);

        // 0 1 2 !
        if(messageType>=0 && messageType<=2) {


            StandardNodeInfo info = ioChannel.attachment();

            long receiveTime = cluster.clusterTime();
            int messageId = Utils.getInt(data);
            int serviceId = Utils.getInt(data);
            long sendTime = Utils.getLong(data);
            byte serviceHeader = Utils.getByte(data);


            if(info.state().is(NodeState.CLUSTER_CONNECTED) ||
                    (info.state().is(NodeState.LEAVING) &&
                            (serviceId==1||serviceId==0))) {

                //ByteBuffer[] duplicate = Utils.duplicate(data, true);


                if (serviceManager.handleMessageEvent(new MessageEvent(

                        data,
                        serviceHeader,
                        serviceId,
                        messageId,
                        sendTime,
                        receiveTime,
                        messageType,
                        info
                ))) {

                    info.setLastMessageId(priority, messageId, receiveTime);

                }
            }else
            {

                logger.warning("Node {} trying to communicate but not ready for it , service-id : {}" ,
                        info , serviceId);
                return;
            }


        }
        else if(messageType== MessageTypes.CLUSTER_MESSAGE) {
            clusterController.put(ClusterLevelEvent.dataEvent(ioChannel, new ByteBufferArray(data)));
        }
        else if(messageType == MessageTypes.ACKNOWLEDGE) {
            StandardNodeInfo info = ioChannel.attachment();
            final byte count = Utils.getByte(data);
            for(int i=0;i<count;i++) {
                final byte ackPriority = Utils.getByte(data);
                final boolean newRound = Utils.getByte(data) == 1;
                int messageId = Utils.getInt(data);


                if (newRound)
                    acknowledgeHandler.
                            handleAcknowledge(
                                    new AcknowledgeResponse(Integer.MAX_VALUE
                                            , info, ackPriority));
                acknowledgeHandler.
                        handleAcknowledge(
                                new AcknowledgeResponse(messageId
                                        , info, ackPriority));
            }
        } else if(messageType == MessageTypes.ALIVE_REQUEST) {

            int reqId = Utils.getInt(data);
            ByteBuffer resp = ByteBuffer.allocate(5);
            resp.put(MessageTypes.ALIVE_RESPONSE).putInt(reqId).flip();
            try {
                ioChannel.send(new ByteBuffer[]{resp} , MAXIMUM_PRIORITY);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        else
        {
            logger.warning("a message with unrecognized header received - message skipped");
        }
    }



    private final void doCreateCluster() throws Throwable
    {
        assertIfStateNotValidForJoinOrCreate();
        initCurrentNodeInfo();
        runDataChannel();


        runClusterEventHandler();
        cluster.clusterEventImpl().start();
        cluster.setLocalClusterPointTime();
        currentNodeInfo.setState(NodeState.CONNECTING);
        currentNodeInfo.setId(1);
        currentNodeInfo.setState(NodeState.SYNCHRONIZING);
        currentNodeInfo.setJoinTime(cluster.clusterTime());
        currentNodeInfo.setState(NodeState.CONNECTED);
        currentNodeInfo.setHashId(Hash.generateSecureHashString());
        nodesList.addNode(currentNodeInfo);
        ifFalse("nodes not synced" ,
                nodesList.setNextLeader() == currentNodeInfo);
        cluster.setState(ClusterState.SYNCHRONIZED);

        acknowledgeHandler.start();
        runMessageServiceEventHandler();

    }


    private void assertIfStateNotValidForJoinOrCreate()
    {
        if(disposed)
            throw new IllegalStateException("this node disposed");
        if(connectedToCluster)
            throw new IllegalStateException("this node already connected to cluster");
    }

    private void assertIfStateNotValidForLeave()
    {
        if(disposed)
            throw new IllegalStateException("this node disposed");

        if(!connectedToCluster)
            throw new IllegalStateException("this node not connected to any cluster yet");
    }


    void sendValidate(int serviceId)
    {
        if(disposed) throw new IllegalStateException("this node disposed");

        if(leaving && serviceId>1)
            throw new IllegalStateException("this node is leaving cluster");

        if(connectedToCluster)return;


        throw new IllegalStateException("node not connected to any cluster yet");
    }


    Timer sharedTimer() {
        return sharedTimer;
    }


    private final boolean runSafe(Runnable runnable , String methodName)
    {
        try{
            logger.information("run safe {}" , methodName);
            runnable.run();
            return true;
        }catch (Throwable e)
        {

            logger.warning("trying to run safe "+methodName+" but an exception occurs", e);

            return false;
        }
    }

    PriorityLock priorityLock() {
        return priorityLock;
    }

    JoinHashCalculator hashCalculator() {
        return hashCalculator;
    }

    @Override
    public String toString() {
        String idAsString = currentNodeInfo==null?
                "NA":currentNodeInfo.id()==-1?
                "NA":String.valueOf(currentNodeInfo.id());
        return "Node{id:"+idAsString+"}";
    }


    /**
     * created just for test
     * @return iochannel
     */
    public IoChannelCenter getIoChannelCenter() {
        return ioChannelCenter;
    }
}
