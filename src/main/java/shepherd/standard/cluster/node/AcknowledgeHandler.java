package shepherd.standard.cluster.node;

import shepherd.utils.concurrency.threaddispatcher.SynchronizedDispatcher;
import shepherd.utils.concurrency.threaddispatcher.exceptions.DispatcherException;
import shepherd.utils.concurrency.threaddispatcher.simple.syncdispatcher.SimpleSynchronizedDispatcher;

class AcknowledgeHandler {

    private SynchronizedDispatcher<Byte , AcknowledgeResponse> responseHandler;

    public AcknowledgeHandler()
    {
        responseHandler = new SimpleSynchronizedDispatcher<>(1 , this::handle);
    }

    private void handle(byte priority , AcknowledgeResponse response)
    {
        response.handle();
    }

    void handleAcknowledge(AcknowledgeResponse ack)
    {
        responseHandler.tryDispatch(ack.priority() , ack);
    }

    void start()
    {
        responseHandler.start();
    }

    void stop()
    {
        responseHandler.terminateAndWaitToFinish();
    }

    void setNumberOfAckHandlerThreads(int threads)
    {
        try {
            responseHandler.setNumberOfHandlerThreads(threads);
        } catch (DispatcherException e) {
            throw new IllegalStateException(e);
        }
    }
}
