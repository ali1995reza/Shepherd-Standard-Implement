package shepherd.utils.concurrency.threaddispatcher.abstractmodel.dispatcher;

import shepherd.utils.collection.ArrayBlockingDeque;
import shepherd.utils.concurrency.threaddispatcher.Dispatcher;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public abstract class AbstractDispatcher<T , QT> implements Dispatcher<T> {


    private List<Thread> threads;
    private Consumer<T> consumer;
    private final ArrayBlockingDeque queue;
    private final Object _dispatchSync = new Object();
    private ThreadFactory threadFactory;
    private Object _sync = new Object();
    private boolean isActive = false;
    private boolean isTerminated = false;
    private int numberOfHandlerThreads = 0;
    private CountDownLatch stopLatch = null;
    private boolean synchronousTerminate;
    private final Object threadFinishToken = new Object();


    public AbstractDispatcher(int numberOfThreads , Consumer<T> co , ThreadFactory factory)
    {

        if(numberOfThreads<=0)
            throw new IllegalArgumentException("size must be bigger or equal 1");

        if(factory == null)
            throw new NullPointerException("thread factory can not be null");

        threads = new ArrayList<>();
        this.threadFactory = factory;
        queue = new ArrayBlockingDeque();
        consumer = co;
        for(int i=0;i<numberOfThreads;i++)
        {
            threads.add(createThread());
        }

        numberOfHandlerThreads = numberOfThreads;


        initialize(queue);
    }

    public AbstractDispatcher(int numberOfThreads , Consumer<T> co)
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

    public AbstractDispatcher(Consumer<T> co)
    {
        this(1,co);
    }


    protected abstract void initialize(Deque<QT> deque);


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

                    queue.addFirst(threadFinishToken);
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

        synchronized (_sync) {
            assertIfTerminated();
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
    public void dispatch(T data) throws InterruptedException {

        synchronized (_dispatchSync) {
            assertIfTerminated();

            if (!putInQueue(data)) {
                throw new IllegalStateException("can not add to queue");
            }
        }

    }


    protected abstract boolean putInQueue(T data);

    private Thread createThread()
    {
        return threadFactory.newThread(this::mainLoop);
    }

    @Override
    public boolean tryDispatch(T data) {
        synchronized (_dispatchSync) {

            if (isTerminated)
                return false;

            return putInQueue(data);
        }
    }

    @Override
    public void start()
    {
        synchronized (_sync)
        {
            assertIfTerminated();
            assertIfActive("dispatcher already is isActive");

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

                Object queueData = queue.takeFirst();

                //System.out.println(queue.size());
                if (queueData == threadFinishToken) {
                    synchronized (_sync) {
                        threads.remove(Thread.currentThread());
                    }
                    if (stopLatch != null) {
                        stopLatch.countDown();
                    }

                    return;
                }
                try {

                    computeData((QT)queueData , consumer);
                } catch (Throwable e) {
                    e.printStackTrace();
                }

                if(synchronousTerminate)
                    return;

            } catch (InterruptedException e) {
                //never happens
            }
        }
    }


    protected abstract void computeData(QT queueData , Consumer<T> consumer);



    @Override
    public List<T> terminate() {

        final boolean insideHandlerThread;
        synchronized (_sync)
        {
            synchronized (_dispatchSync) {
                assertIfTerminated();

                insideHandlerThread = isCurrentThreadAHandlerThread();

                isTerminated = true;

                if (!isActive) {
                    //so dont need to wait for threads
                    for (Thread t : threads) {
                        t.interrupt();
                    }

                    return getRemainingData();
                }

                final int totalCallAbleThreads = threads.size() - (insideHandlerThread ? 1 : 0);
                stopLatch = new CountDownLatch(totalCallAbleThreads);
                for (int i = 0; i < totalCallAbleThreads; i++) {
                    queue.addLast(threadFinishToken);
                }
            }
        }


        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            while (stopLatch.getCount()>0);
        }

        synchronousTerminate = true;
        return getRemainingData();

    }

    @Override
    public void terminateAndWaitToFinish() {

        final boolean insideOfHandlerThread;
        synchronized (_sync)
        {
            synchronized (_dispatchSync) {
                assertIfTerminated();

                assertIfNotActive("dispatcher not active to handle data");

                //assertIfInHandlerThread("can not use this method inside of handler threads");

                insideOfHandlerThread = isCurrentThreadAHandlerThread();

                isTerminated = true;


                stopLatch = new CountDownLatch(threads.size());
                for (int i = 0; i < numberOfHandlerThreads; i++) {
                    queue.addLast(threadFinishToken);
                }
            }
        }


        if(insideOfHandlerThread)
        {
            mainLoop();
        }

        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            while (stopLatch.getCount()>0);
        }

        synchronousTerminate = true;


        return;
    }

    @Override
    public void setConsumer(Consumer<T> consumer) {
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
        synchronized (_sync)
        {
            return isActive;
        }
    }



    protected abstract List<T> getRemainingData();


    private void assertIfActive(String msg)
    {
        if(isActive)
            throw new IllegalStateException(msg);
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


    protected final boolean isTerminatedSynchronous()
    {
        return synchronousTerminate;
    }

}
