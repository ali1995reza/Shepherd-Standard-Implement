package shepherd.standard.cluster.node;

import shepherd.api.asynchronous.AsynchronousResultListener;
import shepherd.api.message.MessageMetadata;
import shepherd.api.message.ack.Acknowledge;
import shepherd.api.message.exceptions.MessageException;

interface QuestionResponseHandler<T> {


    MessageMetadata handle(T response, QuestionImpl<T> question, boolean waitForAck, AsynchronousResultListener<Acknowledge> listener) throws MessageException;
}
