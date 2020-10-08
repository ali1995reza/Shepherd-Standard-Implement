package shepherd.utils.transport.nio.implement;

import shepherd.utils.concurrency.common.ThreadConfig;
import shepherd.utils.transport.nio.model.IoContext;
import shepherd.utils.transport.nio.model.IoHandler;
import shepherd.utils.transport.nio.model.IoProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class NIOProcessor implements IoProcessor {




    private boolean active;
    private boolean stopped;
    private Object _sync = new Object();
    private IoThread[] ioThreads;
    private int numberOfIOThreads;
    private IoContextImpl context;
    private IdleDetector idleDetector;
    private final ThreadGroup ioThreadsGroup = new ThreadGroup(this+" : io-threads-group");


    public NIOProcessor(int noit , ThreadConfig threadConfig)
    {
        if(noit<1)
            throw new IllegalStateException("number of io threads must bigger or equal 1");

        if(threadConfig==null)
            throw new NullPointerException("thread config is null");


        idleDetector = new IdleDetector();


        numberOfIOThreads = noit;
        ioThreads = new IoThread[numberOfIOThreads];
        context = new IoContextImpl(this);
        createThreads(adaptedThreadFactory(threadConfig , ioThreadsGroup));

    }

    public NIOProcessor(ThreadConfig config)
    {
        this(1 , config);
    }

    public NIOProcessor(int i)
    {
        this(i, new ThreadConfig(Thread.MAX_PRIORITY , true));
    }

    public NIOProcessor()
    {
        this(1);
    }


    @Override
    public void start()
    {
        synchronized (_sync)
        {
            assertIfStopped();
            assertIfActive();

            for(int i=0;i<numberOfIOThreads;i++)
            {
                try {
                    ioThreads[i].start();
                }catch (Throwable e)
                {
                    for(int j=0;j<i;j++)
                    {
                        ioThreads[j].stop();
                    }

                    stopped = true;
                    throw e;
                }
            }

            idleDetector.start();

            active = true;

        }
    }


    @Override
    public List<IoHandler> stop()
    {
        synchronized (_sync)
        {
            assertIfStopped();

            stopped = true;

        }


        List<IoHandler> handlers = new ArrayList<>();

        for(int i=0;i<numberOfIOThreads;i++)
        {
            ioThreads[i].stop(handlers);
        }

        idleDetector.stop();

        return handlers;
    }





    @Override
    public void registerIoHandler(IoHandler handler) throws IOException {


        synchronized (_sync)
        {

            checkState();
            provideIoThread().registerIoHandler(handler);

        }
    }

    @Override
    public IoContext ioContext() {
        return context;
    }


    public void setIdleCheckTime(long time) {

        idleDetector.setCheckTime(time);
    }

    @Override
    public boolean isInOneOfIoThreads() {
        return Thread.currentThread().getThreadGroup() == ioThreadsGroup;
    }


    private IoThread provideIoThread()
    {
        IoThread t = ioThreads[0];
        for(int i=1;i<numberOfIOThreads;i++)
        {
            if(t.size()>ioThreads[i].size())
            {
                t = ioThreads[i];
            }
        }

        return t;
    }


    private final void assertIfStopped()
    {

        if(stopped)
            throw new IllegalStateException("processor already stopped");
    }

    private final void assertIfNotActive()
    {

        if(!active)
            throw new IllegalStateException("processor not active");
    }

    private final void assertIfActive()
    {

        if(active)
            throw new IllegalStateException("processor is active already");
    }

    private void checkState()
    {
        assertIfStopped();
        assertIfNotActive();
    }

    public void setBlockingSelector(boolean s)
    {
        synchronized (_sync)
        {
            assertIfStopped();
            for(IoThread t:ioThreads)
            {
                t.setBlockingSelector(s);
            }
        }
    }


    private final void createThreads(ThreadFactory factory)
    {
        for(int i=0;i<numberOfIOThreads;i++)
        {
            try {
                ioThreads[i] = new IoThread(context, factory , idleDetector);
            }catch (Throwable e)
            {
                for(int j=0;j<i;j++)
                {
                    ioThreads[j].stop();
                    throw e;
                }
            }
        }
    }


    private static final ThreadFactory adaptedThreadFactory(ThreadConfig config , final ThreadGroup group)
    {
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(group , r);
                if(config.getName()!=null)t.setName(config.getName());
                t.setPriority(config.getPriority());
                t.setContextClassLoader(config.getClassLoader());
                t.setUncaughtExceptionHandler(config.getExceptionHandler());
                t.setDaemon(config.isDaemon());
                return t;
            }
        };
    }


}
