package shepherd.standard.cluster.node;

import shepherd.api.asynchronous.AsynchronousResultListener;
import shepherd.api.message.MessageMetadata;
import shepherd.api.message.ack.Acknowledge;
import shepherd.utils.concurrency.waitobject.IWaitObject;

import java.util.function.Consumer;

class AcknowledgeImpl implements IWaitObject<AcknowledgeImpl> , Acknowledge {

    private int numberOfFailedAcks;
    private int numberOfSuccessAcks;
    private int possibleAcks;
    private int requiredAcks;
    private boolean done;
    private MessageMetadata messageMetadata;
    private final AsynchronousResultListener<Acknowledge> listener;



    public AcknowledgeImpl(MessageMetadata messageMetadata , AsynchronousResultListener<Acknowledge> listener ,
                           int rA ,
                           int pA ) {
        requiredAcks = rA;
        possibleAcks = pA;
        this.listener = listener;
        this.messageMetadata = messageMetadata;
        if(requiredAcks ==0) {
            done  = true;
            callOnComplete();
        }

    }


    public AcknowledgeImpl(AsynchronousResultListener<Acknowledge> listener ,
                           int rA ,
                           int pA ) {

        this(null , listener , rA , pA);

    }


    @Override
    public IWaitObject afterNotifiedDo(Consumer<AcknowledgeImpl> l) {
        throw new IllegalStateException("not implemented !");
    }

    void setMessageMetadata(MessageMetadata messageMetadata) {
        this.messageMetadata = messageMetadata;
    }

    @Override
    public void notifyObject(int code) {
        synchronized (this)
        {
            if(code==1) ++numberOfSuccessAcks;
            else if(code==0) ++numberOfFailedAcks;

            if(done) {
                return;
            }

            if (numberOfSuccessAcks >= requiredAcks ||
                    numberOfSuccessAcks + numberOfFailedAcks == possibleAcks)
            {
                done = true;
            }
        }

        if(done) callOnComplete();
        else callOnUpdate();
    }


    private void callOnComplete()
    {
        try{
            listener.onCompleted(this);
        }catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    private void callOnUpdate()
    {
        try{
            listener.onUpdated(this);
        }catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public int numberOfFailedAcks() {
        return numberOfFailedAcks;
    }

    @Override
    public int numberOfSuccessAcks() {
        return numberOfSuccessAcks;
    }

    @Override
    public MessageMetadata messageMetadata() {
        return messageMetadata;
    }

    @Override
    public int numberOfPossibleAcks(){return numberOfSuccessAcks;}

    @Override
    public int numberOfRequiredAcks() {
        return requiredAcks;
    }

    @Override
    public String toString() {
        return "Acknowledge{" +
                "numberOfFailedAcks=" + numberOfFailedAcks +
                ", numberOfSuccessAcks=" + numberOfSuccessAcks +
                ", possibleAcks=" + possibleAcks +
                ", requiredAcks=" + requiredAcks +
                ", done=" + done +
                ", messageMetadata=" + messageMetadata +
                '}';
    }
}
