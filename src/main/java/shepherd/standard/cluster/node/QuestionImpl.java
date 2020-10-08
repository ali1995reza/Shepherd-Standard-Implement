package shepherd.standard.cluster.node;

import shepherd.api.asynchronous.AsynchronousResultListener;
import shepherd.api.cluster.Cluster;
import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.message.MessageMetadata;
import shepherd.api.message.Question;
import shepherd.api.message.ack.Acknowledge;
import shepherd.api.message.exceptions.MessageException;

class QuestionImpl<T> extends MessageImpl<T> implements Question<T> {

    private final int id;
    private final QuestionResponseHandler<T> responseHandler;
    private Boolean done = false;
    private final int timeOut;
    //just here to calculateTime of this shit !
    private final Cluster cluster;

    QuestionImpl(T messageData
            , int messageId
            , NodeInfo messageSender
            , byte messageType
            , long messageSendTime
            , long messageReceiveTime
            , int questionId
            , int questionTimeOut
            , Cluster c
            , QuestionResponseHandler<T> handler) {
        super(messageData , messageId , messageSender , messageType, messageSendTime , messageReceiveTime);
        id = questionId;
        responseHandler = handler;
        timeOut = questionTimeOut;
        cluster = c;
    }

    boolean isDone()
    {
        return done;
    }

    int questionId()
    {
        return id;
    }

    @Override
    public MessageMetadata response(T response, AsynchronousResultListener<Acknowledge> listener) throws MessageException {
        return response(response , true , listener);
    }

    @Override
    public MessageMetadata response(T response, boolean waitFoAck, AsynchronousResultListener<Acknowledge> listener) throws MessageException {
        synchronized (done)
        {
            if(done)
                throw new MessageException("this question answered before");

            if(timeOut!=-1 && cluster.clusterTime()-metadata().receiveTime()>timeOut)
                throw new MessageException("question timed out");

            try{
                MessageMetadata metadata = responseHandler.handle(response , this , waitFoAck , listener);
                done = true;
                return metadata;
            }catch (Throwable e)
            {
                throw new MessageException(e);
            }
        }
    }

    @Override
    public int timeOut() {
        return timeOut;
    }
}
