package shepherd.utils.concurrency.threaddispatcher.accumulator.syncdispatcher;

import shepherd.utils.concurrency.threaddispatcher.SyncConsumer;
import shepherd.utils.concurrency.threaddispatcher.abstractmodel.syncdispatcher.AbstractSyncDispatcher;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class AccumulatorSynchronizedDispatcher<I , T> extends AbstractSyncDispatcher<I , T , DispatchAccumulator<T>> {


    private final DispatchAccumulator.Builder<T> accumulatorBuilder =
            new DispatchAccumulator.Builder<>();

    private DispatchAccumulator<T> currentAccumulator;
    private I currentSyncId;




    public AccumulatorSynchronizedDispatcher(int numberOfThreads, SyncConsumer<I, T> co, ThreadFactory factory) {
        super(numberOfThreads, co, factory);
    }

    public AccumulatorSynchronizedDispatcher(int numberOfThreads, SyncConsumer<I, T> co) {
        super(numberOfThreads, co);
    }

    public AccumulatorSynchronizedDispatcher(SyncConsumer<I, T> co) {
        super(co);
    }

    @Override
    protected boolean putInQueue(I id, T data, Deque<DispatchAccumulator<T>> queue) {

        DispatchAccumulator<T> accumulator = queue.peekLast();


        if(accumulator==null)
        {
            accumulator = accumulatorBuilder.build();
            accumulator.accumulate(data);
            queue.addLast(accumulator);
        }else if(!accumulator.accumulate(data))
        {
            accumulator = accumulatorBuilder.build();
            accumulator.accumulate(data);
            queue.addLast(accumulator);
        }



        return true;
    }

    @Override
    protected void computeData(I syncId, DispatchAccumulator<T> queueData, SyncConsumer<I, T> consumer) {

        currentSyncId = syncId;
        currentAccumulator = queueData;

        for(T data:queueData)
        {
            try{
                consumer.accept(syncId , data);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }

            if(isTerminatedSynchronous())
            {
                return;
            }
        }

        currentSyncId = null;
        currentAccumulator = null;

    }

    @Override
    protected List<T> getRemainingData(I syncId, Deque<DispatchAccumulator<T>> queue) {

        List<T> list = new ArrayList<>();

        if(currentSyncId!=null && syncId == currentSyncId)
        {
            for(T d:currentAccumulator)
            {
                list.add(d);
            }

        }


        DispatchAccumulator<T> accumulator = queue.pollFirst();

        while (accumulator!=null)
        {
            for(T d:accumulator)
            {
                list.add(d);
            }
            accumulator = queue.pollFirst();
        }

        System.out.println(list.size());


        return list;

    }
}
