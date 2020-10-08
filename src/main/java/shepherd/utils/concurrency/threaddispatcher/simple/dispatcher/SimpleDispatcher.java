package shepherd.utils.concurrency.threaddispatcher.simple.dispatcher;

import shepherd.utils.concurrency.threaddispatcher.abstractmodel.dispatcher.AbstractDispatcher;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public class SimpleDispatcher<T> extends AbstractDispatcher<T , T> {

    private Deque<T> queue;


    public SimpleDispatcher(int numberOfThreads, Consumer<T> co, ThreadFactory factory) {
        super(numberOfThreads, co, factory);
    }

    public SimpleDispatcher(int numberOfThreads, Consumer<T> co) {
        super(numberOfThreads, co);
    }

    public SimpleDispatcher(Consumer<T> co) {
        super(co);
    }

    @Override
    protected void initialize(Deque<T> deque) {
        this.queue = deque;
    }


    @Override
    protected boolean putInQueue(T data) {
        queue.addLast(data);
        return true;
    }

    @Override
    protected void computeData(T queueData, Consumer<T> consumer) {
        consumer.accept(queueData);

    }



    @Override
    protected List<T> getRemainingData() {

        List<T> data = new ArrayList<>();

        T t = queue.poll();

        while (t!=null)
        {
            data.add(t);
            t = queue.poll();
        }

        return data;
    }


}
