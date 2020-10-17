package shepherd.standard.cluster.node;

import shepherd.api.asynchronous.AsynchronousResultListener;
import shepherd.api.cluster.ClusterState;
import shepherd.api.cluster.node.NodeState;
import shepherd.api.config.ConfigurationKey;
import shepherd.api.logger.Logger;
import shepherd.api.logger.LoggerFactory;
import shepherd.api.message.*;
import shepherd.api.message.exceptions.MessageException;
import shepherd.api.message.exceptions.SerializeException;
import shepherd.standard.cluster.node.clusterlevelmessage.*;
import shepherd.standard.datachannel.IoChannel;
import shepherd.standard.message.standardserializer.ObjectSerializer;
import shepherd.standard.utils.TimerThread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.UUID;

import static shepherd.standard.cluster.node.ClusterProtocolConstants.CLSTR_MSG_SRLIZR;
import static shepherd.standard.cluster.node.ClusterProtocolConstants.createSuccessJoinResponse;

public class ClusterController extends TimerThread {

    private final static class DistributeSetToken implements AsynchronousResultListener<Answer<Object>>
    {
        private final JoinResponse response;
        private final IoChannel channel;

        private DistributeSetToken(JoinResponse response, IoChannel channel) {
            this.response = response;
            this.channel = channel;
        }


        private final static boolean checkAnswerState(Answer<Object> answer)
        {
            System.err.println(answer);
            if(answer.responses().size()!=answer.numberOfRequiredResponses())
                return false;

            for(Response response:answer.responses().values())
            {
                if(response.hasError())
                    return false;

                if(response.data().equals(No.NO))
                    return false;
            }

            return true;
        }

        @Override
        public void onUpdated(Answer result) {

        }

        @Override
        public void onCompleted(Answer answer) {


            if(checkAnswerState(answer))
            {
                try {
                    ByteBuffer[] data = CLSTR_MSG_SRLIZR.serialize(response);
                    channel.send(data , (byte)10);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (SerializeException e) {
                    e.printStackTrace();
                }
            }else
            {
                try {
                    ByteBuffer[] data = CLSTR_MSG_SRLIZR.serialize(
                            JoinResponse.FAIL_DUE_STATE_RESPONSE
                    );
                    channel.send(data ,
                            (byte)10);
                    channel.flushAndClose();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (SerializeException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private final static int MAXIMUM_MESSAGE_SERVICE_ID = 0;
    private final static int MINIMUM_MESSAGE_SERVICE_ID = 1;

    private final static byte MAXIMUM_PRIORITY = 10;
    private final static byte MINIMUM_PRIORITY = 1;



    private final StandardNode node;
    private final MessageService maximumPriorityMessageService;
    private final MessageService minimumPriorityMessageService;

    private final MessageListener messageListener = new MessageListener() {
        @Override
        public void onMessageReceived(Message message) {
            put(message);
        }

        @Override
        public void onQuestionAsked(Question question) {
            put(question);
        }
    };

    private boolean leaving = false;
    private boolean stop = false;
    private ClusterConnectToken connectToken;
    private final Object _sync = new Object();

    private final Logger logger;

    private final ClusterStateTracker stateTracker;

    private final StandardCluster cluster;



    ClusterController(StandardNode node)
    {
        super(1000l);
        this.node = node;

        HashMap<ConfigurationKey, Object> conf = new HashMap<>();
        conf.put(MessageServiceConfiguration.PRIORITY , MAXIMUM_PRIORITY);
        maximumPriorityMessageService = node.messageServiceManager()
                .registerService(MAXIMUM_MESSAGE_SERVICE_ID,
                        new ObjectSerializer(true),
                        messageListener ,
                        conf);

        conf.put(MessageServiceConfiguration.PRIORITY , MINIMUM_PRIORITY);
        minimumPriorityMessageService = node.
                messageServiceManager().registerService(MINIMUM_MESSAGE_SERVICE_ID,
                new ObjectSerializer<>(true),
                messageListener ,
                conf);

        stateTracker = new ClusterStateTracker(this.node);
        cluster = (StandardCluster) node.cluster();
        //stateTracker.setOnAnnounceDone();

        logger = LoggerFactory.factory().getLogger(this);

    }


    private void handleQuestion(Question question)
    {
        if(question.data() instanceof LeaveQuestion)
        {
            try {
                handleLeaveAcceptQuestion(question);
            } catch (Exception e) {
                //do a shit here !
            }
        }else if(question.data() instanceof ClusterConnectToken)
        {

            this.connectToken = (ClusterConnectToken) question.data();
            try {
                question.response(Yes.YES , AsynchronousResultListener.EMPTY);
            } catch (MessageException e) {
                logger.warning(e);
            }

            cluster.setState(ClusterState.SYNCHRONIZING);

        }else {
            logger.warning("an unrecognized question received {}" , question);
        }
    }

    private void handleLeaveAcceptQuestion(Question question) throws Exception
    {
        logger.information("leave question asked - node {}" , question.metadata().sender());
        if(question.metadata().sender().state().is(NodeState.LEAVING))
        {
            logger.information("leave accept question accepted for node [{}]" ,
                    question.metadata().sender().id());
            question.response(Yes.YES , AsynchronousResultListener.EMPTY);
            StandardNodeInfo info = (StandardNodeInfo) question.metadata().sender();
            info.setState(NodeState.LEFT);
        }else
        {
            logger.information("leave accept question rejected for node [{}]" ,
                    question.metadata().sender().id());
            question.response(No.NO, AsynchronousResultListener.EMPTY);
        }
    }

    private void handleMessage(Message message)
    {
        if(message.data() instanceof LeaveAnnounce)
        {
            //so want to leave !

            StandardNodeInfo info =
                    (StandardNodeInfo) message.metadata().sender();

            info.setState(NodeState.LEAVING);
        }else if(message.data() instanceof DisconnectAnnounce)
        {
            stateTracker.announceDisconnect(message);
            cluster.setState(ClusterState.SYNCHRONIZING);

        }else if(message.data() instanceof ConnectAnnounce)
        {
            stateTracker.announceConnect(message);
            cluster.setState(ClusterState.SYNCHRONIZING);

        }else{
            logger.warning("an unrecognized message received {}" , message);
        }
    }

    public void handleClusterLevelEvent(ClusterLevelEvent event)
    {
        synchronized (_sync) {

            if(stop)return;

            try {

                if(event.is(ClusterLevelEvent.Type.DISCONNECT))
                    handleDisconnectEvent(event);
                else if(event.is(ClusterLevelEvent.Type.CONNECT))
                    handleConnectEvent(event);
                else if(event.is(ClusterLevelEvent.Type.DATA_RECEIVED))
                    handleDataEvent(event);
            } catch (Throwable e) {
                logger.warning(e);
            }
        }
    }



    private void handleConnectEvent(ClusterLevelEvent event)
    {

        StandardNodeInfo info = new StandardNodeInfo(-1
                , -1
                , null, false , false ,
                node , event.channel());


        event.channel().attach(info);


        info.setState(NodeState.CONNECTING);
        return;
    }


    private void handleDisconnectEvent(ClusterLevelEvent event) throws Throwable {


        StandardNodeInfo info  = event.channel().attachment();
        if(info==null){
            logger.warning("a channel without attached information disconnected");
            return;
        }

        if (info.state().isOneOf(NodeState.UNKNOWN , NodeState.CONNECTING)) {
            info.setState(NodeState.DISCONNECTED);
            return;
        }

        boolean left = info.state().is(NodeState.LEFT);
        info.setState(NodeState.DISCONNECTED);

        node.releaseAllEnqueuedAcknowledgesListener(info);


        logger.information("disconnect announce sent for node [{}]" , info.id());
        maximumPriorityMessageService.
                sendMessage(
                        new DisconnectAnnounce(info.toSerializableInfo(), left),
                        MessageService.DefaultArguments.NO_ACKNOWLEDGE,
                        AsynchronousResultListener.EMPTY);

        stateTracker.localDisconnectAnnounce(info.toSerializableInfo() , left);
    }


    private void handleDataEvent(ClusterLevelEvent event){
        //todo handle it please !
        try {
            parseAndHandleData(event);
        } catch (Throwable e) {
            logger.error(e);
            event.channel().closeNow();
            return;
        }


    }

    private boolean parseAndHandleData(ClusterLevelEvent event) throws Throwable
    {

        Object data = CLSTR_MSG_SRLIZR.deserialize(event.data().buffers());


        if(data instanceof JoinRequest)
        {
            JoinRequest joinRequest = (JoinRequest) data;
            //so handleBy it please !
            StandardNodeInfo nodeInfo = event.channel().attachment();



            if(nodeInfo.state().isNot(NodeState.CONNECTING))
            {
                logger.warning("a node with state that is not CONNECTING send a cluster level event data - Node : {}" , nodeInfo);
                return true;
            }


            if(node.info().isLeader())
            {
                if(!validateJoinRequest(joinRequest))
                {
                    event.channel().closeNow();
                    return true;

                }
                //todo handleBy password



                int newId = node.clusterNextId();
                StandardNodeInfo info = event.channel().attachment();
                info.setId(newId);
                info.setAddress(new NodeSocketAddress(joinRequest.nodeAddress()));
                info.setJoinTime(node.cluster().clusterTime());

                String token = UUID.randomUUID().toString();

                ClusterConnectToken clusterConnectToken = new ClusterConnectToken()
                        .setInfo(info.toSerializableInfo())
                        .setToken(token)
                        .setCreatedTime(node.cluster().clusterTime())
                        .setToken(token);


                JoinResponse response =
                        createSuccessJoinResponse(
                                info,
                                node.cluster().schema().nodes().values() ,
                                token);


                connectToken = clusterConnectToken;

                maximumPriorityMessageService.askQuestion(
                        clusterConnectToken,
                        MessageService.DefaultArguments.ALL_RESPONSES,
                        new DistributeSetToken(response, event.channel())
                );

                cluster.setState(ClusterState.SYNCHRONIZING);


            }else
            {

                //for now just skip this please !

                StandardNodeInfo leaderNode = node.currentClusterLeader();


                event.channel().send(CLSTR_MSG_SRLIZR.serialize(
                        new Redirect(leaderNode.address().address()
                        ))
                        , MAXIMUM_PRIORITY);


                event.channel().flushAndClose();
                return true;
            }
        }else if(data instanceof ConnectRequest)
        {
            ConnectRequest request = (ConnectRequest) data;

            //check token then !

            if(!validateConnectRequest(request))
            {

                event.channel().send(
                        CLSTR_MSG_SRLIZR
                                .serialize(new ConnectResponse().setSuccess(false)) ,
                        MAXIMUM_PRIORITY
                );

                event.channel().flushAndClose();

                return true;
            }


            StandardNodeInfo info = event.channel().attachment();
            info.setAddress(new NodeSocketAddress(request.info().address()));
            info.setId(request.info().id());
            info.setJoinTime(request.info().joinTime());

            ConnectAnnounce announce = new ConnectAnnounce(request.info());

            maximumPriorityMessageService.sendMessage(
                    announce ,
                    MessageService.DefaultArguments.NO_ACKNOWLEDGE ,
                    AsynchronousResultListener.EMPTY
            );

            stateTracker.localConnectAnnounce(
                    event.channel() ,
                    announce);

        }else if(data instanceof ReadyAnnounce)
        {
            ReadyAnnounce announce = (ReadyAnnounce) data;
            StandardNodeInfo info = event.channel().attachment();
            node.nodesList().addNode(info);

            if(!announce.joinHash().equals(info.hashId()))
            {
                event.channel().closeNow();
                return true;
            }


            info.setState(NodeState.CLUSTER_CONNECTED);

            return true;
        }else if(data instanceof TimeSync)
        {
            TimeSync timeSync = (TimeSync) data;
            timeSync.setClusterTime(node.cluster().clusterTime());
            event.channel().send(
                    CLSTR_MSG_SRLIZR.serialize(
                            timeSync
                    ) ,
                    MAXIMUM_PRIORITY
            );
        }else
        {
            logger.warning("an unrecognized cluster level message has been detected - Message : {}" , data);
            return true;
        }


        return false;
    }

    private boolean validateJoinRequest(JoinRequest request)
    {
        if(request.password()==null || request.password().isEmpty()){
            logger.warning("a join request with empty password detected");
            return false;
        }

        if(cluster.state().isNot(ClusterState.SYNCHRONIZED))
        {
            logger.warning("a join request received but rejected cause cluster state is {}" , cluster.state());
            return false;
        }

        for(StandardNodeInfo nodeInfo:node.nodesList().immutableList())
        {
            if(nodeInfo.address().address().equals(request.nodeAddress())) {
                logger.warning("a join request with exists address detected , address : {}" ,request.nodeAddress());
                return false;
            }
        }

        return true;

    }

    private boolean validateConnectRequest(ConnectRequest request)
    {
        if(connectToken==null) {
            logger.warning("connect token is null but a connect request received");
            return false;
        }

        if(connectToken.createdTime()+connectToken.expireTimeOut()<
                node.cluster().clusterTime())
        {
            logger.warning("connect token is timed out , token : {}" , connectToken.token());
            connectToken = null;
            return false;
        }

        if(connectToken.validate(request))
        {
            logger.information("connect request is valid , token : {}" , connectToken.token());
            connectToken = null;
            return true;
        }

        logger.warning("connect request invalid and not match with current token , current token : {}", connectToken.token());
        return false;
    }


    private void checkStateTracker()
    {

    }

    @Override
    protected void init() {
    }

    @Override
    protected void onDataAvailable(Object data) {
        if(data instanceof Question)
        {
            handleQuestion((Question)data);
        }else if(data instanceof Message)
        {
            handleMessage((Message)data);
        }else if(data instanceof ClusterLevelEvent)
        {
            handleClusterLevelEvent((ClusterLevelEvent)data);
        }

        checkStateTracker();
    }


    @Override
    protected void onWakeup() {
        checkStateTracker();
    }

    @Override
    protected void onException(Throwable e) {

    }

    @Override
    protected void onStop() {

    }
}
