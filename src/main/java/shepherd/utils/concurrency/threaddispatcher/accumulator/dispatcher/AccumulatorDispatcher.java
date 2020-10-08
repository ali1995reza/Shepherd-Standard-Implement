package shepherd.utils.concurrency.threaddispatcher.accumulator.dispatcher;

import shepherd.utils.concurrency.threaddispatcher.abstractmodel.dispatcher.AbstractDispatcher;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public class AccumulatorDispatcher<T> extends AbstractDispatcher<T , ConcurrentDispatchAccumulator<T>> {


    private final ConcurrentDispatchAccumulator.Builder<T> accumulatorBuilder =
            new ConcurrentDispatchAccumulator.Builder<>();
    private Deque<ConcurrentDispatchAccumulator<T>> queue;
    private ConcurrentDispatchAccumulator<T> current;


    public AccumulatorDispatcher(int numberOfThreads, Consumer<T> co, ThreadFactory factory) {
        super(numberOfThreads, co, factory);
    }

    public AccumulatorDispatcher(int numberOfThreads, Consumer<T> co) {
        super(numberOfThreads, co);
    }

    public AccumulatorDispatcher(Consumer<T> co) {
        super(co);
    }

    @Override
    protected void initialize(Deque<ConcurrentDispatchAccumulator<T>> queue) {
        this.queue = queue;
    }


    @Override
    protected boolean putInQueue(T data) {
        ConcurrentDispatchAccumulator<T> accumulator =  queue.peekLast();

        if(accumulator==null)
        {
            accumulator = accumulatorBuilder.build();
            accumulator.accumulate(data);
            queue.addLast(accumulator);
        }else
        {
            if(!accumulator.accumulate(data))
            {
                accumulator = accumulatorBuilder.build();
                if(accumulator.accumulate(data)) {
                    queue.addLast(accumulator);
                }else
                {
                    throw new IllegalStateException("its never must happens !");
                }
            }
        }

        return true;
    }

    @Override
    protected void computeData(ConcurrentDispatchAccumulator<T> queueData, Consumer<T> consumer) {

        this.current = queueData;
        for(T data:queueData)
        {
            try{
                consumer.accept(data);
            }catch (Throwable e)
            {
                e.printStackTrace();

            }

            if(isTerminatedSynchronous())
            {
                return;
            }
        }
        current = null;

    }

    @Override
    protected List<T> getRemainingData() {

        List<T> list = new ArrayList<>();

        if(current!=null)
        {
            for(T t:current)
            {
                list.add(t);
            }
        }
        ConcurrentDispatchAccumulator<T> accumulator = queue.pollFirst();

        while (accumulator!=null)
        {
            for(T t:accumulator)
            {
                list.add(t);
            }
            accumulator = queue.poll();
        }

        return list;
    }


}
