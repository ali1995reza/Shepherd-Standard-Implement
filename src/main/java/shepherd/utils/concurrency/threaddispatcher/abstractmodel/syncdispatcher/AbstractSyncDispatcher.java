package shepherd.utils.concurrency.threaddispatcher.abstractmodel.syncdispatcher;

import shepherd.utils.collection.ArrayBlockingDeque;
import shepherd.utils.concurrency.threaddispatcher.SyncConsumer;
import shepherd.utils.concurrency.threaddispatcher.SynchronizedDispatcher;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public abstract class AbstractSyncDispatcher<I , T , QT> implements
        SynchronizedDispatcher<I,T> , DispatchQueueMethods<I , T , QT> {




    final SyncDispatchSequence stopSequence = new SyncDispatchSequence(null , -1);

    private final Function<I, SyncDispatchSequence<I , T , QT>> creator = new Function<I, SyncDispatchSequence<I , T , QT>>() {
        @Override
        public SyncDispatchSequence<I , T , QT> apply(I categoryId) {

            synchronized (_sync)
            {
                if(isTerminated)
                    return null;


                return new SyncDispatchSequence(AbstractSyncDispatcher.this , categoryId);
            }
        }
    };



    private List<Thread> threads;
    private SyncConsumer<I , T> consumer;
    private final ArrayBlockingDeque<SyncDispatchSequence<I , T , QT>> queue;
    private ThreadFactory threadFactory;
    private final Object _sync = new Object();
    private boolean isActive = false;
    private boolean isTerminated = false;
    private CountDownLatch stopLatch = null;
    private ConcurrentHashMap<I , SyncDispatchSequence<I , T , QT>> sequences;
    private int numberOfHandlerThreads;
    private boolean synchronousTerminate;


    public AbstractSyncDispatcher(int numberOfThreads , SyncConsumer<I , T> co , ThreadFactory factory)
    {

        if(numberOfThreads<=0)
            throw new IllegalArgumentException("size must be bigger or equal 1");

        if(factory == null)
            throw new NullPointerException("thread factory can not be null");

        threads = new ArrayList<>();
        this.threadFactory = factory;
        queue = new ArrayBlockingDeque<>();
        consumer = co;
        for(int i=0;i<numberOfThreads;i++)
        {
            threads.add(createThread());
        }
        sequences = new ConcurrentHashMap<>();
        numberOfHandlerThreads = numberOfThreads;
    }

    public AbstractSyncDispatcher(int numberOfThreads , SyncConsumer<I , T> co)
    {
        this(numberOfThreads, co, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            }
        });
    }

    public AbstractSyncDispatcher(SyncConsumer<I , T> co)
    {
        this(1,co);
    }

    protected final void shrink(int how) {
        synchronized (_sync)
        {
            if(how>=numberOfHandlerThreads)
            {
                throw new IllegalArgumentException("can not shrink - there are just "
                        +numberOfHandlerThreads+" running");
            }

            if(how<1)
                throw new IllegalArgumentException("shrink must bigger or equal 1");

            if(isActive) {

                for (int i = 0; i < how; i++) {
                    queue.addFirst(stopSequence);
                }
            }else
            {
                for(int i=0;i<how;i++)
                {
                    threads.remove(i).interrupt();
                }
            }

            numberOfHandlerThreads-=how;

        }

    }

    protected final void extend(int how)
    {
        synchronized (_sync)
        {

            if(how<1)
                throw new IllegalArgumentException("extend argument must be bigger or equal with 1");

            for(int i=0;i<how;i++)
            {
                Thread t = createThread();
                threads.add(t);
                if(isActive)
                    t.start();
            }

            numberOfHandlerThreads+=how;
        }
    }

    @Override
    public void setNumberOfHandlerThreads(int n) {

        if(n<1)
            throw new IllegalArgumentException("number of handler threads at least must be 1");


        synchronized (_sync)
        {
            if(n>numberOfHandlerThreads)
            {
                extend(n-numberOfHandlerThreads);
            }else if(n<numberOfHandlerThreads)
            {
                shrink(numberOfHandlerThreads-n);
            }
        }
    }

    @Override
    public void dispatch(I syncId, T data) throws InterruptedException {


        assertIfTerminated();

        SyncDispatchSequence<I, T, QT> sequence = getOrCreateSequence(syncId);

        if(sequence==null)
            assertIfTerminated();

        if (!sequence.putNext(data)) {
            throw new IllegalStateException("can not add to handler queue at this time");
        }
    }


    @Override
    public boolean tryDispatch(I syncId, T data) {


        if (isTerminated)
            return false;

        SyncDispatchSequence<I, T, QT> sequence = getOrCreateSequence(syncId);

        if(sequence==null)
            return false;


        return sequence.putNext(data);
    }

    private Thread createThread()
    {
        return threadFactory.newThread(this::mainLoop);
    }



    private SyncDispatchSequence<I , T , QT> getOrCreateSequence(I i)
    {
        return sequences.computeIfAbsent(i, creator);
    }


    @Override
    public final void addToHandlerQueue(SyncDispatchSequence<I, T, QT> s) {
        queue.addLast(s);
    }


    protected abstract boolean putInQueue(I id, T data, Deque<QT> queue);

    @Override
    public final boolean putInSyncQueue(I id, T data, Deque<QT> queue) {
        return putInQueue(id , data , queue);
    }


    @Override
    public void start()
    {
        synchronized (_sync)
        {
            assertIfTerminated();
            assertIfActive();

            if(consumer==null)
                throw new IllegalStateException("data consumer can not be null");

            for(Thread t:threads)
            {
                t.start();
            }

            isActive = true;

        }

    }


    private void mainLoop()
    {

        while (true) {
            try {

                SyncDispatchSequence<I , T , QT> sequence = queue.takeFirst();
                if (sequence == stopSequence) {
                    synchronized (_sync) {
                        threads.remove(Thread.currentThread());
                    }
                    if (stopLatch != null) {
                        stopLatch.countDown();
                    }

                    return;
                }
                try {


                    computeData(sequence.syncId , sequence.getNext() , consumer);
                } catch (Throwable e) {
                    e.printStackTrace();
                }



                if(synchronousTerminate)
                    return;


                sequence.enqueueForNext();


            } catch (InterruptedException e) {
                //never happens
            }
        }
    }


    protected abstract void computeData(I syncId , QT queueData , SyncConsumer<I , T> consumer);


    @Override
    public Map<I , List<T>> terminate() {
        synchronized (_sync)
        {
            assertIfTerminated();

            isTerminated = true;



        }

        if (!isActive) {
            //so dont need to wait for threads
            for (Thread t : threads) {
                t.interrupt();
            }

            return getRemainingData();
        }


        for(SyncDispatchSequence sequence:sequences.values())
        {
            sequence.close();
        }


        stopLatch = new CountDownLatch(threads.size()-(isCurrentThreadAHandlerThread()?1:0));
        for(int i=0;i<numberOfHandlerThreads;i++)
        {
            queue.addFirst(stopSequence);
        }

        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            while (stopLatch.getCount()>0);
        }


        synchronousTerminate = true;

        Map<I , List<T>> remaining = getRemainingData();



        return remaining;

    }

    @Override
    public void terminateAndWaitToFinish() {

        final  boolean isInHandlerThread;
        synchronized (_sync)
        {
            assertIfTerminated();
            assertIfNotActive("dispatcher not active to handle remaining data");

            isInHandlerThread = isCurrentThreadAHandlerThread();


            isTerminated = true;

        }


        CountDownLatch sequenceStopLatch = new CountDownLatch(sequences.size());

        for (SyncDispatchSequence sequence : sequences.values()) {
            sequence.close(sequenceStopLatch);
        }


        if(isInHandlerThread)
        {

            //find the sequence at this thread
            final Thread currentThread = Thread.currentThread();
            for(SyncDispatchSequence sequence:sequences.values())
            {
                if(currentThread == sequence.controllerThread())
                {

                    sequence.enqueueForNext();
                    break;
                }
            }

            handleUntilLatchActive(sequenceStopLatch);

        }else {
            try {
                sequenceStopLatch.await();
            } catch (InterruptedException e) {
                while (sequenceStopLatch.getCount() > 0) ;
            }
        }

        stopLatch = new CountDownLatch(numberOfHandlerThreads-(isInHandlerThread?1:0));
        for(int i=0;i<numberOfHandlerThreads;i++)
        {
            queue.addLast(stopSequence);
        }


        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            while (stopLatch.getCount()>0);
        }

        synchronousTerminate = true;

    }

    @Override
    public void setConsumer(SyncConsumer<I , T> consumer) {
        assertIfTerminated();

        if(consumer==null)
            throw new NullPointerException();

        this.consumer = consumer;
    }

    @Override
    public int numberOfThreads() {
        return numberOfHandlerThreads;
    }

    @Override
    public boolean isActive() {

        return isActive;
    }


    private final Map<I , List<T>> getRemainingData()
    {
        HashMap<I , List<T>> remaining = new HashMap<>();

        Set<I> keysSet = sequences.keySet();

        for (I key :keysSet)
        {
            SyncDispatchSequence<I , T, QT> sequence = sequences.get(key);

            List<T> list = getRemainingData(key  , sequence.getQueue());

            remaining.put(key , list);
        }

        return remaining;
    }

    protected abstract List<T> getRemainingData(I syncId , Deque<QT> queue);


    private void assertIfActive()
    {
        if(isActive)
            throw new IllegalStateException("dispatcher already is isActive");
    }

    private void assertIfTerminated()
    {
        if(isTerminated)
            throw new IllegalStateException("dispatcher already terminated");
    }

    private void assertIfNotActive(String msg)
    {
        if(!isActive)
            throw new IllegalStateException(msg);
    }




    private final boolean isCurrentThreadAHandlerThread()
    {
        Thread currentThread = Thread.currentThread();

        for(Thread thread:threads)
        {
            if(currentThread==thread) return true;
        }

        return false;
    }


    private void assertIfInHandlerThread(String msg)
    {
        if(isCurrentThreadAHandlerThread())
        {
            throw new IllegalStateException(msg);
        }
    }


    private final void handleUntilLatchActive(CountDownLatch conditionLatch)
    {

        while (conditionLatch.getCount()>0) {
            try {

                SyncDispatchSequence<I , T , QT> sequence = queue.pollFirst(10 , TimeUnit.MILLISECONDS);

                if(sequence==null)continue;


                if (sequence == stopSequence) {
                    synchronized (_sync) {
                        threads.remove(Thread.currentThread());
                    }
                    if (stopLatch != null) {
                        stopLatch.countDown();
                    }

                    return;
                }


                try {
                    computeData(sequence.syncId , sequence.getNext() , consumer);
                } catch (Throwable e) {
                    e.printStackTrace();
                }



                sequence.enqueueForNext();

            } catch (InterruptedException e) {
                //never happens
            }
        }
    }



    protected final boolean isTerminatedSynchronous()
    {
        return synchronousTerminate;
    }
}
