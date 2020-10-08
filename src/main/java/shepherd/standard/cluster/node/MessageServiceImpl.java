package shepherd.standard.cluster.node;

import shepherd.standard.config.ConfigChangeResult;
import shepherd.api.asynchronous.AsynchronousResultListener;
import shepherd.api.cluster.ClusterEvent;
import shepherd.api.cluster.ClusterEventListener;
import shepherd.api.cluster.ClusterState;
import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.cluster.node.NodeState;
import shepherd.api.config.ConfigurationChangeResult;
import shepherd.api.config.ConfigurationKey;
import shepherd.api.config.IConfiguration;
import shepherd.api.logger.Logger;
import shepherd.api.logger.LoggerFactory;
import shepherd.api.message.*;
import shepherd.api.message.ack.Acknowledge;
import shepherd.api.message.exceptions.MessageException;
import shepherd.utils.buffer.Utils;

import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class MessageServiceImpl<T> implements MessageService<T> {

    private static class SyncClusterEvent implements MessageServiceEvent{

        enum Type{
            NODE_STATE_CHANGED , CLUSTER_STATE_CHANGED , LEADER_CHANGED
        }

        private final int serviceId;
        private final Type type;



        private SyncClusterEvent(int sId , Type t)
        {
            serviceId = sId;
            type = t;
        }



        @Override
        public int serviceId() {
            return serviceId;
        }

        @Override
        public void handleBy(MessageServiceImpl service) {

            service.handleCustomEvent(this);
        }
    }

    private final static class ClusterStateChanged extends SyncClusterEvent{

        private final ClusterState lastState;
        private final ClusterState currentState;


        private ClusterStateChanged(int sId , ClusterState l , ClusterState c) {
            super(sId, Type.CLUSTER_STATE_CHANGED);
            lastState = l;
            currentState = c;
        }

    }

    private final static class NodeStateChanged extends SyncClusterEvent{

        private final NodeState lastState;
        private final NodeState currentState;
        private final NodeInfo node;

        private NodeStateChanged(int sId ,NodeInfo info ,  NodeState l , NodeState c) {
            super(sId, Type.NODE_STATE_CHANGED);
            lastState = l;
            currentState = c;
            node = info;
        }
    }

    private final static class LeaderChanged extends SyncClusterEvent{

        private final NodeInfo node;


        private LeaderChanged(int sId, NodeInfo node) {
            super(sId, Type.LEADER_CHANGED);
            this.node = node;
        }
    }

    private final static class ResponseTimeOut implements MessageServiceEvent{


        private final int serviceId;
        private final int questionId;

        private ResponseTimeOut(int serviceId , int questionId)
        {
            this.serviceId =serviceId;
            this.questionId = questionId;
        }



        @Override
        public int serviceId() {
            return serviceId;
        }

        @Override
        public void handleBy(MessageServiceImpl service) {
            service.handleCustomEvent(this);
        }
    }

    private final static class QuestionFailedNode implements MessageServiceEvent{

        private final int serviceId;
        private final int questionId;
        private final int failedNodes;

        private QuestionFailedNode(int serviceId , int questionId, int failedNodes)
        {
            this.serviceId =serviceId;
            this.questionId = questionId;
            this.failedNodes = failedNodes;
        }



        @Override
        public int serviceId() {
            return serviceId;
        }

        @Override
        public void handleBy(MessageServiceImpl service) {
            service.handleCustomEvent(this);
        }
    }


    private final class AnswerAckListener implements AsynchronousResultListener<Acknowledge> {

        private final int questionId;

        private AnswerAckListener(int questionId) {
            this.questionId = questionId;
        }

        @Override
        public void onUpdated(Acknowledge result) {

        }

        @Override
        public void onCompleted(Acknowledge result) {
            if(result.numberOfFailedAcks()>0) {
                syncQueue.enqueue(new QuestionFailedNode(serviceId,
                        questionId ,
                        result.numberOfFailedAcks()));
            }
        }
    };


    final public static class ResponseErrorTypes
    {
        public final static byte NO_ERROR = Byte.MIN_VALUE;
        public final static byte DESTINATION_DOSE_NOT_SUPPORT_SERVICE = Byte.MIN_VALUE+1;
        public final static byte QUESTION_DESERIALIZE_HAD_ERROR = Byte.MIN_VALUE+2;
        public final static byte RESPONSE_DESERIALIZE_HAD_ERROR = Byte.MIN_VALUE+3;

        public static Response.Error findErrorByTypeId(byte tId)
        {
            if(tId==NO_ERROR)
            {
                return null;
            }else if(tId== DESTINATION_DOSE_NOT_SUPPORT_SERVICE)
            {
                return Response.Error.DESTINATION_DOSE_NOT_SUPPORT_SERVICE;
            }else if(tId == QUESTION_DESERIALIZE_HAD_ERROR)
            {
                return Response.Error.QUESTION_DESERIALIZE_HAD_ERROR;
            }else if(tId == RESPONSE_DESERIALIZE_HAD_ERROR)
            {
                return Response.Error.RESPONSE_DESERIALIZE_HAD_ERROR;
            }

            throw new IllegalArgumentException("invalid error type id");
        }
    }

    private final static class ClusterEventSynchronizer implements ClusterEventListener{

        private final MessageServiceImpl service;
        private final int serviceId;


        private ClusterEventSynchronizer(MessageServiceImpl service)
        {
            this.service = service;
            this.serviceId = this.service.serviceId;
        }


        @Override
        public void onClusterStateChanged(ClusterState lastState, ClusterState currentState) {
            service.syncQueue.enqueue(new ClusterStateChanged(serviceId , lastState , currentState));
        }

        @Override
        public void onNodeStateChanged(NodeInfo node, NodeState lastState, NodeState currentState) {
            service.syncQueue.enqueue(new NodeStateChanged(serviceId , node , lastState , currentState));

        }

        @Override
        public void onLeaderChanged(NodeInfo leader) {
            service.syncQueue.enqueue(new LeaderChanged(serviceId , leader));
        }
    }

    private final class QuestionTimeOutTask extends TimerTask
    {
        private final int questionId;
        public QuestionTimeOutTask(int qId)
        {
            questionId = qId;
        }

        @Override
        public void run() {
            syncQueue.enqueue(new ResponseTimeOut(serviceId , questionId));
        }
    }


    private final static class StatisticsImpl implements MessageServiceStatistics {

        private final MessageServiceImpl service;

        private StatisticsImpl(MessageServiceImpl service) {
            this.service = service;

        }

        @Override
        public long totalSentMessages() {
            return service.totalSentMessages;
        }

        @Override
        public long totalReceivedMessages() {
            return service.totalReceivedMessages;
        }

        @Override
        public long totalSentQuestions() {
            return service.totalSentQuestions;
        }

        @Override
        public long totalReceivedQuestions() {
            return service.totalReceivedQuestions;
        }

        @Override
        public long totalSentResponses() {
            return service.totalSentResponses;
        }

        @Override
        public long totalReceivedResponses() {
            return service.totalReceivedResponses;
        }
    }







    final static MessageSerializer<ByteBuffer[]> BYTE_BUFFER_ARRAY_SERIALIZER =
            new MessageSerializer<ByteBuffer[]>() {
                @Override
                public ByteBuffer[] serialize(ByteBuffer[] msgData) {
                    return msgData;
                }

                @Override
                public ByteBuffer[] deserialize(ByteBuffer[] rawData) {
                    return rawData;
                }
            };



    final static byte MESSAGE  = 0;
    final static byte QUESTION = 1;
    final static byte QUESTION_RESPONSE = 2;


    //this is the important thing !
    private MessageSerializer<T> serializer;
    private MessageDispatcher messageDispatcher;
    private final int serviceId;
    private MessageListener<T> listener;
    private ConcurrentHashMap<Integer , AnswerImpl<T>> answers;
    private AtomicInteger questionIdGenerator = new AtomicInteger(Integer.MIN_VALUE);
    //shared timer for calculate answer timeout !
    private Timer sharedTimer;
    private MessageServiceSyncQueue syncQueue;
    private ClusterEventImpl clusterEvent;
    private IConfiguration configuration;

    private long totalSentMessages;
    private long totalReceivedMessages;

    private long totalSentQuestions;
    private long totalReceivedQuestions;

    private long totalSentResponses;
    private long totalReceivedResponses;

    private final StatisticsImpl statistics;

    private final Logger logger;

    MessageServiceImpl(StandardNode node ,
                       MessageDispatcher dispatcher ,
                       int serviceId ,
                       MessageSerializer<T> serializer ,
                       MessageListener<T> l ,
                       Timer sTimer ,
                       MessageServiceSyncQueue syncQueue)
    {
        this.messageDispatcher = dispatcher;
        this.serviceId = serviceId;
        this.serializer = serializer;
        this.listener = l;
        answers = new ConcurrentHashMap<>();
        sharedTimer = sTimer;
        this.syncQueue = syncQueue;
        clusterEvent = new ClusterEventImpl(false);
        ((ClusterEventImpl)node.cluster().clusterEvent())
                .addClusterEventListenerOnTop(new ClusterEventSynchronizer(this));

        configuration = node.configurations()
                .createSubConfiguration("MessageService_"+id());

        configuration.defineConfiguration(
                MessageServiceConfiguration.PRIORITY ,
                messageDispatcher.priority() ,
                this::approveChange
        );

        statistics = new StatisticsImpl(this);

        logger = LoggerFactory.factory().getLogger(this);
    }


    @Override
    public MessageMetadata sendMessage(T message, int requiredAcknowledges, AsynchronousResultListener<Acknowledge> listener) throws MessageException {
        try {
            if(message == null)
                throw new IllegalArgumentException("message can not be null");

            ByteBuffer[] data = serializer.serialize(message);
            MessageMetadata metadata =
                    messageDispatcher.broadcastData(MESSAGE, data, requiredAcknowledges, listener);

            ++totalSentMessages;

            return metadata;
        }catch (Throwable e)
        {
            throw new MessageException(e);
        }
    }

    @Override
    public MessageMetadata sendMessage(T message, int[] nodes, int requiredAcknowledges, AsynchronousResultListener<Acknowledge> listener) throws MessageException {
        try {

            if(message == null)
                throw new IllegalArgumentException("message can not be null");

            ByteBuffer[] data = serializer.serialize(message);
            MessageMetadata metadata =
                    messageDispatcher.multicastData(MESSAGE, data, requiredAcknowledges ,nodes , listener);

            ++totalSentMessages;

            return metadata;
        }catch (Throwable e)
        {
            throw new MessageException(e);
        }
    }






    @Override
    public MessageMetadata askQuestion(T question, int[] nodes, int requiredResponses, AsynchronousResultListener<Answer<T>> listener) throws MessageException {
        return askQuestion(question  , nodes  , requiredResponses, -1 ,listener);
    }

    @Override
    public MessageMetadata askQuestion(T question, int[] nodes, int requiredResponses, int timeOut, AsynchronousResultListener<Answer<T>> listener) throws MessageException {
        try {

            if(question==null)
                throw new IllegalArgumentException("question can not be null");

            if(requiredResponses == DefaultArguments.ALL_RESPONSES)
            {
                requiredResponses = nodes.length;
            }else if(requiredResponses == DefaultArguments.HALF_RESPONSES)
            {
                requiredResponses = nodes.length/2+nodes.length%2;
            }

            if(requiredResponses <1)
                throw new IllegalStateException("number of needed responses at least must be 1");
            if(requiredResponses >nodes.length)
                throw new IllegalStateException("number of needed responses bigger than number of destinations");

            ByteBuffer[] serializedQuestion = serializer.serialize(question);
            int questionId = newQuestionId(100000);
            ByteBuffer header = ByteBuffer.allocateDirect(8);
            header.putInt(questionId);

            if(timeOut>0)
                header.putInt(timeOut);
            else
                header.putInt(-1);

            header.flip();


            serializedQuestion = Utils.combine(header , serializedQuestion);

            AnswerImpl<T> answer = new AnswerImpl<>(listener , nodes.length , requiredResponses);
            answers.put(questionId , answer);

            MessageMetadata metadata = null;

            try {
                metadata = messageDispatcher.multicastData(QUESTION, serializedQuestion, nodes.length, nodes , new AnswerAckListener(questionId));

                answer.setQuestionMetadata(metadata);

                if(timeOut>0)
                {
                    QuestionTimeOutTask timeOutTask = new
                            QuestionTimeOutTask(questionId);
                    sharedTimer.schedule(timeOutTask, timeOut);
                    answer.setTimeoutTask(timeOutTask);
                }

            }catch (Throwable e)
            {
                answers.remove(questionId , answer);
                throw e;
            }


            ++totalSentQuestions;

            return metadata;
        }catch (Throwable e)
        {
            throw new MessageException(e);
        }
    }

    @Override
    public MessageMetadata askQuestion(T question, int requiredResponses, int timeOut, AsynchronousResultListener<Answer<T>> listener) throws MessageException {
        try {
            if(question == null)
                throw new IllegalArgumentException("question data can not be null");

            ByteBuffer[] serializedQuestion = serializer.serialize(question);
            int questionId = newQuestionId(100000);
            ByteBuffer header = ByteBuffer.allocateDirect(8);
            header.putInt(questionId);

            if(timeOut>0)
                header.putInt(timeOut);
            else
                header.putInt(-1);

            header.flip();


            serializedQuestion = Utils.combine(header , serializedQuestion);

            AnswerImpl<T> answer = new AnswerImpl<>(listener);
            answers.put( questionId , answer);

            MessageMetadata metadata = null;


            try {

                metadata = messageDispatcher.broadcastData(QUESTION, serializedQuestion,
                        DefaultArguments.ALL_ACKNOWLEDGES,
                        answer , requiredResponses , new AnswerAckListener(questionId));

                if(timeOut>0)
                {
                    QuestionTimeOutTask timeOutTask = new QuestionTimeOutTask(questionId);
                    sharedTimer.schedule(timeOutTask , timeOut);
                    answer.setTimeoutTask(timeOutTask);
                }

            }catch (Throwable e)
            {
                answers.remove(questionId , answer);
                throw e;
            }


            ++totalSentQuestions;

            return metadata;
        }catch (Throwable e)
        {
            throw new MessageException(e);
        }
    }

    @Override
    public MessageMetadata askQuestion(T question, int requiredResponses, AsynchronousResultListener<Answer<T>> listener) throws MessageException {

        return askQuestion(question , requiredResponses, -1 , listener);
    }

    @Override
    public ClusterEvent synchronizedClusterEvent() {
        return clusterEvent;
    }

    @Override
    public IConfiguration configurations() {
        return configuration.asUndefinableConfiguration();
    }


    private final ConfigurationChangeResult approveChange(IConfiguration parent ,
                                                          ConfigurationKey key ,
                                                          Object val  ,
                                                          Object oldVal)
    {

        return ConfigChangeResult.newFailResult("not handled yet");
    }

    void handleReceivedData(final ByteBuffer[] data ,
                            final byte serviceHeader ,
                            final int messageId ,
                            final NodeInfo messageSender ,
                            final byte messageType ,
                            final long sendTime ,
                            final long receiveTime)
    {
        if(serviceHeader == MESSAGE) {

            try {

                T messageData = serializer.deserialize(data);
                MessageImpl<T> message = new MessageImpl<>(messageData, messageId, messageSender, messageType, sendTime, receiveTime);

                ++totalReceivedMessages;

                callOnMessageReceived(message);

            } catch (Throwable e) {
                logger.warning(e);
            }
        }else if(serviceHeader == QUESTION)
        {
            try {

                int questionId = Utils.getInt(data);
                int timeOut = Utils.getInt(data);
                T messageData = null;
                try {
                     messageData = serializer.deserialize(data);
                }catch (Throwable e)
                {
                    //todo send error resp to other node !
                    ByteBuffer resp = ByteBuffer.allocate(5);
                    resp.putInt(questionId).put(ResponseErrorTypes.QUESTION_DESERIALIZE_HAD_ERROR).flip();
                    messageDispatcher.multicastData(QUESTION_RESPONSE , new ByteBuffer[]{resp} , 0 , new int[]{messageSender.id()} , AsynchronousResultListener.EMPTY);
                    return;
                    //so some error on Deserialize Data !
                }

                QuestionImpl<T> question =
                        new QuestionImpl<T>(messageData, messageId, messageSender,
                                messageType, sendTime, receiveTime ,
                                questionId , timeOut , messageDispatcher.cluster() , this::handleQuestionResponse);


                ++totalReceivedQuestions;

                callOnQuestionAsked(question);


            } catch (Throwable e) {
                //handleBy question deserialize error

                logger.warning(e);
            }
        }else if(serviceHeader == QUESTION_RESPONSE)
        {
            try {

                int questionId = Utils.getInt(data);
                byte responseErrorType = Utils.getByte(data);
                AnswerImpl<T> answer = answers.get(questionId);

                ++totalReceivedResponses;

                if (answer == null) {
                    //so this question timed-out
                    //todo save log maybe
                    //return

                    return;
                }
                ResponseImpl<T> response = null;
                if (responseErrorType == ResponseErrorTypes.NO_ERROR) {

                    try {

                        T responseData = serializer.deserialize(data);
                        response = new ResponseImpl<T>(responseData, messageId, messageSender, messageType, sendTime, receiveTime, null);

                    }catch (Throwable e)
                    {
                        response = new ResponseImpl<T>(null , messageId , messageSender , messageType , sendTime , receiveTime , Response.Error.RESPONSE_DESERIALIZE_HAD_ERROR);
                    }



                } else {

                    response = new ResponseImpl<T>(null, messageId, messageSender, messageType, sendTime, receiveTime, ResponseErrorTypes.findErrorByTypeId(responseErrorType));
                }

                if (answer.putNewResponse(response)) {
                    answers.remove(questionId, answer);
                    cancelTimeOutTask(answer);
                }
            } catch (Throwable e) {
                //handleBy question deserialize error
                //e.printStackTrace();
                logger.warning(e);
            }
        }else
        {
            logger.warning("an unrecognized header received , header = {}" , serviceHeader);
        }

    }


    private final void handleCustomEvent(MessageServiceEvent event)
    {
        if(event instanceof ResponseTimeOut)
        {
            ResponseTimeOut responseTimeOut = (ResponseTimeOut)event;
            AnswerImpl<T> answer = answers.get(responseTimeOut.questionId);
            if(answer==null)
            {
                //its answered before !
                return;
            }
            if(answer.setTimedOut())
            {
                answers.remove(responseTimeOut.questionId , answer);
                cancelTimeOutTask(answer);
            }
        } else if(event instanceof QuestionFailedNode) {
            QuestionFailedNode fEvent = (QuestionFailedNode)event;

            AnswerImpl<T> answer = answers.get(fEvent.questionId);
            if(answer==null)return;

            if(answer.addFailedNodes(fEvent.failedNodes))
            {
                answers.remove(fEvent.questionId , answer);
                cancelTimeOutTask(answer);
            }

        } else if(event instanceof ClusterStateChanged)
        {
            ClusterStateChanged cEvent = (ClusterStateChanged)event;
            clusterEvent.notifyClusterStateChanged(cEvent.lastState , cEvent.currentState);
        }else if(event instanceof NodeStateChanged)
        {
            NodeStateChanged nEvent = (NodeStateChanged) event;
            clusterEvent.notifyNodeStateChanged(nEvent.node ,nEvent.lastState ,nEvent.currentState);
        }else if(event instanceof LeaderChanged)
        {
            LeaderChanged lEvent = (LeaderChanged)event;
            clusterEvent.notifyLeaderChanged(lEvent.node);
        }
    }

    private MessageMetadata handleQuestionResponse(T response , QuestionImpl<T> question , boolean waitForAck , AsynchronousResultListener<Acknowledge> listener)
            throws MessageException
    {
        try {
            ByteBuffer[] serializedResponse = serializer.serialize(response);
            ByteBuffer responseHeader = ByteBuffer.allocate(5);
            responseHeader.putInt(question.questionId())
                    .put(ResponseErrorTypes.NO_ERROR)
                    .flip();

            serializedResponse = Utils.combine(
                    responseHeader ,
                    serializedResponse
            );


            MessageMetadata metadata =  messageDispatcher.multicastData(QUESTION_RESPONSE ,
                    serializedResponse , waitForAck?1:0 ,
                    new int[]{question.metadata().sender().id()} , listener);

            ++totalSentResponses;

            return metadata;
        } catch (Throwable e) {
            throw new MessageException(e);
        }
    }

    public int id()
    {
        return serviceId;
    }

    @Override
    public MessageServiceStatistics statistics() {
        return statistics;
    }


    private final void callOnMessageReceived(Message<T> message)
    {
        try{

            listener.onMessageReceived(message);
        }catch (Throwable e)
        {
            e.printStackTrace();
        }
    }


    private final void callOnQuestionAsked(Question<T> question)
    {
        try{

            listener.onQuestionAsked(question);
        }catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    private final static boolean cancelTimeOutTask(AnswerImpl answer)
    {
        TimerTask timoutTask = answer.timeoutTask();
        if(timoutTask==null)return false;
        return timoutTask.cancel();
    }


    private final int newQuestionId(int tryCount){
        int id = questionIdGenerator.incrementAndGet();
        if(!answers.containsKey(id))
            return id;

        for(int i=0;i<tryCount;i++)
        {
            id = questionIdGenerator.incrementAndGet();
            if(!answers.containsKey(id))
                return id;
        }

        throw new IllegalStateException("can't generate new question id");
    }

    @Override
    public String toString() {
        return "MessageService : "+serviceId;
    }
}
