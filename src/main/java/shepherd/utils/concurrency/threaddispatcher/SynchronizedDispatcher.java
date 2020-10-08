package shepherd.utils.concurrency.threaddispatcher;

import shepherd.utils.concurrency.threaddispatcher.exceptions.DispatcherException;

import java.util.List;
import java.util.Map;

public interface SynchronizedDispatcher<I , T> {


    void start();
    void setNumberOfHandlerThreads(int numberOfHandlerThreads)throws DispatcherException;
    boolean tryDispatch(I syncId, T data);
    void dispatch(I syncId, T data) throws InterruptedException;
    Map<I , List<T>> terminate();
    void terminateAndWaitToFinish();
    void setConsumer(SyncConsumer<I, T> consumer);
    int numberOfThreads();
    boolean isActive();
}
