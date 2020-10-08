package shepherd.utils.concurrency.threaddispatcher.simple.syncdispatcher;

import shepherd.utils.concurrency.threaddispatcher.SyncConsumer;
import shepherd.utils.concurrency.threaddispatcher.abstractmodel.syncdispatcher.AbstractSyncDispatcher;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class SimpleSynchronizedDispatcher<I , T> extends AbstractSyncDispatcher<I , T , T> {
    public SimpleSynchronizedDispatcher(int numberOfThreads, SyncConsumer<I, T> co, ThreadFactory factory) {
        super(numberOfThreads, co, factory);
    }

    public SimpleSynchronizedDispatcher(int numberOfThreads, SyncConsumer<I, T> co) {
        super(numberOfThreads, co);
    }

    public SimpleSynchronizedDispatcher(SyncConsumer<I, T> co) {
        super(co);
    }

    @Override
    protected boolean putInQueue(I id, T data, Deque<T> queue) {
        queue.addLast(data);
        return true;
    }

    @Override
    protected void computeData(I syncId, T queueData, SyncConsumer<I, T> consumer) {

        consumer.accept(syncId , queueData);
    }

    @Override
    protected List<T> getRemainingData(I syncId, Deque<T> queue) {

        List<T> list = new ArrayList<>();

        T t = queue.pollFirst();

        while (t!=null)
        {
            list.add( t);
            t = queue.pollFirst();
        }

        return list;
    }


}
