package shepherd.utils.concurrency.threaddispatcher.simple.syncdispatcher;

import shepherd.utils.concurrency.threaddispatcher.SyncConsumer;
import shepherd.utils.concurrency.threaddispatcher.abstractmodel.syncdispatcher.AbstractByteKeySyncDispatcher;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class SimpleByteKeySynchronizedDispatcher<T> extends AbstractByteKeySyncDispatcher<T , T> {

    public SimpleByteKeySynchronizedDispatcher(int numberOfThreads, SyncConsumer<Byte, T> co, ThreadFactory factory) {
        super(numberOfThreads, co, factory);
    }

    public SimpleByteKeySynchronizedDispatcher(int numberOfThreads, SyncConsumer<Byte, T> co) {
        super(numberOfThreads, co);
    }

    public SimpleByteKeySynchronizedDispatcher(SyncConsumer<Byte, T> co) {
        super(co);
    }

    @Override
    protected void computeData(Byte syncId, T queueData, SyncConsumer<Byte, T> consumer) {
        consumer.accept(syncId , queueData);
    }

    @Override
    protected List<T> getRemainingData(Byte syncId, Deque<T> queue) {
        List<T> list = new ArrayList<>();

        T t = queue.pollFirst();

        while (t!=null)
        {
            list.add( t);
            t = queue.pollFirst();
        }

        return list;
    }

    @Override
    protected boolean putInQueue(Byte id, T data, Deque<T> queue) {
        queue.addLast(data);
        return true;
    }
}
