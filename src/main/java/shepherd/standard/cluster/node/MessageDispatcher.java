package shepherd.standard.cluster.node;

import shepherd.api.asynchronous.AsynchronousResultListener;
import shepherd.api.cluster.Cluster;
import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.message.MessageMetadata;
import shepherd.api.message.MessageService;
import shepherd.api.message.ack.Acknowledge;
import shepherd.utils.buffer.Utils;
import shepherd.utils.concurrency.lock.PriorityLock;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

class MessageDispatcher {



    private static final class MessageIDGenerator
    {
        private int messageId;

        MessageIDGenerator()
        {
            messageId = Integer.MIN_VALUE;
        }

        int generate()
        {
            return messageId++;
        }
    }


    static class Builder
    {

        private final StandardNode node;
        private final MessageIDGenerator[] idGenerator;
        private final Consumer<Integer> validator;
        private final PriorityLock lock;

        Builder(StandardNode node , byte numberOfPriorities , Consumer<Integer> validator)
        {
            this.node = node;
            idGenerator = new MessageIDGenerator[numberOfPriorities+1];
            for(int i=1;i<idGenerator.length;i++)
            {
                idGenerator [i] = new MessageIDGenerator();
            }

            this.lock = node.priorityLock();

            this.validator = validator;
        }

        private void assertIfServiceIdNotValid(int serviceId)
        {
            if(serviceId<0)
                throw new IllegalArgumentException("service id must be 0 or positive");
        }

        MessageDispatcher build(final int serviceId , byte priority)
        {
            assertIfServiceIdNotValid(serviceId);
            return new MessageDispatcher(node.nodesList() ,
                    node.info() ,
                    node.cluster() ,
                    idGenerator[priority] ,
                    lock ,
                    serviceId ,
                    priority ,
                    validator);
        }

        MessageDispatcher build(byte priority)
        {
            return new MessageDispatcher(node.nodesList() ,
                    node.info() ,
                    node.cluster() ,
                    idGenerator[priority] ,
                    lock,
                    priority ,
                    validator);
        }




        public void runSynchronized(Runnable r)
        {
            lock.lockMaximumPriority();
            try{
                r.run();
            }finally {
                lock.unlock();
            }
        }
    }


    private final NodesListManager nodesList;
    private final PriorityLock lock;
    private final MessageIDGenerator idGenerator;
    private final Cluster cluster;
    private final NodeInfo currentNodeInfo;
    private final int serviceId;
    private final boolean accessToSendWithAnyServiceId;
    private final byte priority;
    private final Consumer<Integer> validator;
    private final ByteBuffer header;
    private final ByteBuffer readableHeader;

    private MessageDispatcher(NodesListManager list ,
                              NodeInfo current ,
                              Cluster cluster ,
                              MessageIDGenerator generator ,
                              PriorityLock priorityLock ,
                              int serviceId ,
                              byte priority ,
                              Consumer<Integer> validator)
    {
        nodesList = list;
        idGenerator = generator;
        currentNodeInfo = current;
        this.cluster = cluster;
        this.lock = priorityLock;
        this.serviceId = serviceId;
        accessToSendWithAnyServiceId = serviceId==-1;
        this.validator = validator;
        this.priority = priority;

        header = ByteBuffer.allocateDirect(18);
        readableHeader = header.asReadOnlyBuffer();
    }


    private void lock()
    {
        lock.lock(priority);
    }

    private void unlock()
    {
        lock.unlock();
    }

    private MessageDispatcher(NodesListManager list ,
                              NodeInfo current ,
                              Cluster cluster ,
                              MessageIDGenerator generator ,
                              final PriorityLock lock ,
                              byte priority ,
                              Consumer<Integer> validator)
    {
        this(list , current , cluster , generator ,  lock , -1 , priority, validator);
    }


    final MessageMetadata multicastData(final byte serviceHeader  ,
                                        final ByteBuffer[] data ,
                                        int requiredAcknowledges ,
                                        final int []  targets ,
                                        AsynchronousResultListener<Acknowledge> listener)
    {
        if(accessToSendWithAnyServiceId)
            throw new IllegalStateException("service id not specified in this dispatcher");

        return sendData(serviceId , serviceHeader , data , requiredAcknowledges , targets , listener);
    }


    final MessageMetadata broadcastData(final byte serviceHeader ,
                                        final ByteBuffer[] data ,
                                        int requiredAcknowledges ,
                                        AsynchronousResultListener<Acknowledge> listener)
    {
        if(accessToSendWithAnyServiceId)
            throw new IllegalStateException("service id not specified in this dispatcher");

        return sendData(serviceId , serviceHeader , data , requiredAcknowledges , listener);
    }




    final MessageMetadata broadcastData(final byte serviceHeader ,
                                        final ByteBuffer[] data ,
                                        int requiredAcknowledges,
                                        AnswerImpl answer ,
                                        int requiredResponses ,
                                        AsynchronousResultListener<Acknowledge> listener)
    {
        if(accessToSendWithAnyServiceId)
            throw new IllegalStateException("service id not specified in this dispatcher");

        return sendData(serviceId , serviceHeader , data , requiredAcknowledges , answer , requiredResponses , listener);
    }



    final MessageMetadata multicastData(final int serviceId ,
                                        final byte serviceHeader  ,
                                        final ByteBuffer[] data ,
                                        int requiredAcknowledges ,
                                        final int[] targets,
                                        AsynchronousResultListener<Acknowledge> listener)
    {
        if(!accessToSendWithAnyServiceId)
            throw new IllegalStateException("access denied");

        return sendData(serviceId , serviceHeader , data , requiredAcknowledges , targets , listener);
    }


    final MessageMetadata broadcastData(final int serviceId ,
                                        final byte serviceHeader ,
                                        final ByteBuffer[] data ,
                                        int requiredAcknowledges ,
                                        AsynchronousResultListener<Acknowledge> listener)
    {
        if(!accessToSendWithAnyServiceId)
            throw new IllegalStateException("access denied");

        return sendData(serviceId , serviceHeader , data , requiredAcknowledges , listener);
    }




    final MessageMetadata broadcastData(final  int serviceId ,
                                        final byte serviceHeader ,
                                        final ByteBuffer[] data ,
                                        int requiredAcknowledges,
                                        AnswerImpl answer ,
                                        int requiredResponses ,
                                        AsynchronousResultListener<Acknowledge> listener)
    {
        if(!accessToSendWithAnyServiceId)
            throw new IllegalStateException("access denied");

        return sendData(serviceId , serviceHeader , data , requiredAcknowledges , answer , requiredResponses , listener);
    }






    private final MessageMetadata sendData(final int serviceId ,
                                     final byte serviceHeader  ,
                                     final ByteBuffer[] data ,
                                     int requiredAcknowledges ,
                                     final int[] targets ,
                                     AsynchronousResultListener<Acknowledge> listener)
    {

        assertIfDataIsNull(data);

        if(targets==null)
            return sendData(serviceId , serviceHeader, data, requiredAcknowledges ,listener);


        requiredAcknowledges = normalizeAndCheckNumberOfAcknowledges(requiredAcknowledges
                , targets);

        if(requiredAcknowledges>0)
        {
            AcknowledgeImpl acknowledge = new AcknowledgeImpl(listener , requiredAcknowledges, targets.length);
            try {
                return bufferData(serviceId , serviceHeader, targets, data, acknowledge);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

        }
        else if(requiredAcknowledges==0)
        {
            MessageMetadata metadata = null;
            try {
                metadata = bufferData(serviceId , serviceHeader, targets, data);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

            new AcknowledgeImpl(metadata , listener , 0 , targets.length);
            return metadata;
        }
        else
        {
            throw new IllegalArgumentException("number of required acknowledges is negative");
        }

    }

    private final MessageMetadata sendData(final int serviceId ,
                                           final byte serviceHeader ,
                                           final ByteBuffer[] data ,
                                           int requiredAcknowledges ,
                                           AsynchronousResultListener<Acknowledge> listener)
    {

        assertIfDataIsNull(data);

        final NodeInfoImpl[] targets = nodesList.immutableList();
        final int numberOfAllDestinationNodes = targets.length-1;

        requiredAcknowledges = normalizeAndCheckNumberOfAcknowledges(requiredAcknowledges ,
                numberOfAllDestinationNodes);

        if(requiredAcknowledges>0) {
            AcknowledgeImpl acknowledge = new AcknowledgeImpl(listener , requiredAcknowledges, numberOfAllDestinationNodes);
            try {
                return bufferData(serviceId , serviceHeader, targets, data, acknowledge , null);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }else if(requiredAcknowledges==0)
        {
            MessageMetadata metadata = null;
            try {
                metadata = bufferData(serviceId , serviceHeader, targets, data);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

            new AcknowledgeImpl(metadata , listener , 0 ,numberOfAllDestinationNodes);
            return metadata;
        }else {
            throw new IllegalArgumentException("number of required acknowledges is negative");
        }
    }




    private final MessageMetadata sendData(final int serviceId ,
                                             final byte serviceHeader ,
                                             final ByteBuffer[] data ,
                                             int requiredAcknowledges,
                                             AnswerImpl answer ,
                                             int requiredResponses ,
                                             AsynchronousResultListener<Acknowledge> listener)
    {

        assertIfDataIsNull(data);

        final NodeInfoImpl[] targets = nodesList.immutableList();
        final int numberOfAllDestinationNodes = targets.length-1;


        requiredResponses = normalizeAndCheckNumberOfResponses(requiredResponses ,
                numberOfAllDestinationNodes);

        answer.setNumberOfPossibleResponses(numberOfAllDestinationNodes , requiredResponses);

        requiredAcknowledges = normalizeAndCheckNumberOfAcknowledges(requiredAcknowledges  ,
                numberOfAllDestinationNodes);

        if(requiredAcknowledges>0) {
            AcknowledgeImpl acknowledge = new AcknowledgeImpl(listener , requiredAcknowledges, numberOfAllDestinationNodes);

            try {
                return bufferData(serviceId, serviceHeader, targets, data, acknowledge , answer);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }else if(requiredAcknowledges==0)
        {
            MessageMetadata metadata = null;
            try {
                metadata = bufferData(serviceId , serviceHeader, targets, data);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

            answer.setQuestionMetadata(metadata);
            new AcknowledgeImpl(metadata , listener , 0 ,numberOfAllDestinationNodes);

            return metadata;
        }else {
            throw new IllegalArgumentException("number of requested acks is negative - possibles :  [0,n] and "+ MessageService.DefaultArguments.ALL_ACKNOWLEDGES);
        }
    }




    private MessageMetadata bufferData(final int serviceId ,
                            final byte serviceHeader ,
                            final NodeInfoImpl[] targets ,
                            ByteBuffer[] data ,
                            final AcknowledgeImpl acknowledge  ,
                            final AnswerImpl answer)
    {
        lock();
        try {

            validator.accept(serviceId);

            final int messageId = idGenerator.generate();
            final long sendTime = cluster.clusterTime();


            MessageMetadataImpl messageMetadata =
                    new MessageMetadataImpl(
                            messageId ,
                            currentNodeInfo ,
                            MessageMetadata.MessageType.BROADCAST ,
                            sendTime ,
                            -1
                    );

            acknowledge.setMessageMetadata(messageMetadata);
            if(answer!=null)answer.setQuestionMetadata(messageMetadata);

            header.clear();
            header.put(MessageMetadata.MessageType.BROADCAST.code());
            header.putInt(messageId);
            header.putInt(serviceId);
            header.putLong(sendTime);
            header.put(serviceHeader);
            header.flip();

            data = Utils.duplicateAsOnlyReadable(data);

            for (NodeInfoImpl info : targets) {
                if (info == currentNodeInfo)
                    continue;
                else {
                    if (info == null) {
                        acknowledge.notifyObject(0);
                    } else {

                        info.send(getHeader(),
                                Utils.duplicate(data) ,
                                acknowledge ,
                                messageId ,
                                priority ,
                                serviceId);

                    }
                }
            }

            return messageMetadata;


        }finally {
            unlock();
        }

    }


    private MessageMetadata bufferData(final int serviceId ,
                            final byte serviceHeader ,
                            final int[] targets ,
                            ByteBuffer[] data ,
                            final AcknowledgeImpl acknowledge)
    {

        lock();
        try {

            validator.accept(serviceId);

            final int messageId = idGenerator.generate();
            final long sendTime = cluster.clusterTime();



            MessageMetadata.MessageType msgType = targets.length == 1 ?
                    MessageMetadata.MessageType.UNICAST :
                    MessageMetadata.MessageType.MULTICAST;


            MessageMetadataImpl messageMetadata =
                    new MessageMetadataImpl(
                            messageId,
                            currentNodeInfo,
                            msgType,
                            sendTime,
                            -1
                    );

            acknowledge.setMessageMetadata(messageMetadata);

            header.clear();
            header.put(msgType.code());
            header.putInt(messageId);
            header.putInt(serviceId);
            header.putLong(sendTime);
            header.put(serviceHeader);
            header.flip();

            data = Utils.duplicateAsOnlyReadable(data);

            for (int i : targets) {
                if (i == currentNodeInfo.id())
                    acknowledge.notifyObject(0);
                else {

                    NodeInfoImpl info = nodesList.fastFindById(i);

                    if (info == null) {
                        acknowledge.notifyObject(0);
                    } else {

                        info.send(getHeader(),
                                Utils.duplicate(data),
                                acknowledge,
                                messageId ,
                                priority ,
                                serviceId);

                    }
                }
            }

            return messageMetadata;
        }finally {
            unlock();
        }
    }




    private MessageMetadata bufferData(final int serviceId ,
                                       final byte serviceHeader ,
                                       final int[] targets ,
                                       ByteBuffer[] data )
    {
        lock();
        try {

            validator.accept(serviceId);

            int messageId = idGenerator.generate();
            long sendTime = cluster.clusterTime();





            MessageMetadata.MessageType msgType = targets.length==1?
                    MessageMetadata.MessageType.UNICAST :
                    MessageMetadata.MessageType.MULTICAST;

            MessageMetadataImpl messageMetadata =

                    new MessageMetadataImpl(
                            messageId ,
                            currentNodeInfo ,
                            msgType,
                            sendTime ,
                            -1
                    );


            header.clear();
            header.put(msgType.code());
            header.putInt(messageId);
            header.putInt(serviceId);
            header.putLong(sendTime);
            header.put(serviceHeader);
            header.flip();


            data = Utils.duplicateAsOnlyReadable(data);

            for (int i : targets) {
                if (i == currentNodeInfo.id())
                    continue;
                else {

                    NodeInfoImpl info = nodesList.fastFindById(i);

                    if(info!=null)
                    {
                        info.send(getHeader(),
                                Utils.duplicate(data) ,
                                priority ,
                                serviceId);
                    }
                }
            }

            return messageMetadata;
        }finally {
            unlock();
        }


    }



    private MessageMetadata bufferData(final int serviceId ,
                                       final byte serviceHeader ,
                                       final NodeInfoImpl[] targets ,
                                       ByteBuffer[] data)
    {
        lock();
        try {

            validator.accept(serviceId);


            int messageId = idGenerator.generate();
            long sendTime = cluster.clusterTime();


            MessageMetadata messageMetadata =
                    new MessageMetadataImpl(
                    messageId ,
                    currentNodeInfo ,
                    MessageMetadata.MessageType.BROADCAST,
                    sendTime ,
                    -1
            );

            header.clear();
            header.put(ClusterProtocolConstants.MessageTypes.BROADCAST);
            header.putInt(messageId);
            header.putInt(serviceId);
            header.putLong(sendTime);
            header.put(serviceHeader);
            header.flip();


            data = Utils.duplicateAsOnlyReadable(data );

            for (NodeInfoImpl info : targets) {
                if (info == currentNodeInfo)
                    continue;
                else {

                    info.send(getHeader() ,
                            Utils.duplicate(data) ,
                            priority , serviceId);
                }
            }

            return messageMetadata;
        }finally {
            unlock();
        }
    }

    Cluster cluster()
    {
        return cluster;
    }

    private final ByteBuffer getHeader()
    {
        readableHeader.clear();
        return readableHeader;
    }
    //for use in message event handler !


    private final static int normalizeAndCheckNumberOfAcknowledges(int requiredAcknowledges , int[] targets)
    {
        if(targets.length==0)
            throw new IllegalStateException("target len is 0");


        if(targets.length<requiredAcknowledges)
        {
            throw new IllegalStateException("number of required acknowledges is bigger than targets");
        }

        if(requiredAcknowledges == MessageService.DefaultArguments.ALL_ACKNOWLEDGES)
        {
            requiredAcknowledges = targets.length;
        }else if(requiredAcknowledges ==MessageService.DefaultArguments.HALF_ACKNOWLEDGES)
        {
            requiredAcknowledges = targets.length/2+targets.length%2;
        }

        return requiredAcknowledges;
    }


    private final static int normalizeAndCheckNumberOfAcknowledges(int requiredAcknowledges , int numberOfAllDestinationNodes)
    {

        if(numberOfAllDestinationNodes<requiredAcknowledges)
            throw new IllegalStateException("number of required acknowledges is bigger than targets");

        if(requiredAcknowledges == MessageService.DefaultArguments.ALL_ACKNOWLEDGES)
            requiredAcknowledges = numberOfAllDestinationNodes;
        else if(requiredAcknowledges == MessageService.DefaultArguments.HALF_ACKNOWLEDGES)
            requiredAcknowledges = numberOfAllDestinationNodes/2+numberOfAllDestinationNodes%2;

        return requiredAcknowledges;
    }

    private final static int normalizeAndCheckNumberOfResponses(int requiredResponses , int numberOfAllDestinationNodes)
    {

        if(requiredResponses>numberOfAllDestinationNodes)
            throw new IllegalStateException("number of needed responses bigger than number of destination nodes");

        if(requiredResponses == MessageService.DefaultArguments.ALL_RESPONSES)
        {
            requiredResponses = numberOfAllDestinationNodes;
        }else if(requiredResponses == MessageService.DefaultArguments.HALF_RESPONSES)
        {
            requiredResponses = numberOfAllDestinationNodes/2+numberOfAllDestinationNodes%2;
        }

        if(requiredResponses<1 && numberOfAllDestinationNodes>=1)
            throw new IllegalStateException("number of needed responses at least must be 1");

        return requiredResponses;
    }


    public byte priority() {
        return priority;
    }


    private final static void assertIfDataIsNull(Object o)
    {
        if(o==null)
        {
            throw new IllegalArgumentException("data can not be null");
        }
    }

}
