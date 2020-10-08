package shepherd.utils.transport.nio.implement;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

class IdleDetector {


    private Thread maniThread;
    private List<IdleFinder> finders;
    private long checkTime ;

    public IdleDetector(ThreadFactory factory , long checkTime)
    {
        maniThread = factory.newThread(this::mainLoop);
        this.checkTime = checkTime;
        finders = new ArrayList<>();
    }

    public IdleDetector(ThreadFactory factory)
    {
        this(factory , 1000);
    }

    public IdleDetector(long checkTime)
    {
        this(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            }
        }, checkTime);
    }

    public IdleDetector()
    {
        this(1000);
    }

    private final void mainLoop()
    {
        long calculateStart = 0;
        long calculateEnd = 0;
        long wholeCalculateTime = 0;

        while (true)
        {
            //so handle it please !

            wholeCalculateTime = calculateEnd-calculateStart;
            try {
                Thread.sleep(wholeCalculateTime>=checkTime?0:checkTime-wholeCalculateTime);
            } catch (InterruptedException e) {
                break;
            }

            calculateStart = System.currentTimeMillis();

            int numberOfFindersThatFindIdle = 0;
            CountDownLatch latch;
            synchronized (finders)
            {
                if(maniThread.isInterrupted())
                    return;
                int size = finders.size();
                for(int i=0;i<size;i++)
                {
                    if(finders.get(i).check())
                    {
                        ++numberOfFindersThatFindIdle;
                    }
                }


                latch = new CountDownLatch(numberOfFindersThatFindIdle);

                for(int i=0;i<size;i++)
                {
                    IdleFinder finder = finders.get(i);
                    if(finder.setLatchIfIdlesFound(latch))
                    {
                        finder.ioThread().wakeUpIfInBlockingMode();
                    }
                }
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                break;
            }
            calculateEnd = System.currentTimeMillis();
        }
    }

    IdleFinder registerNewIoThread(IoThread t)
    {
        synchronized (finders) {
            IdleFinder finder = new IdleFinder(t);
            finders.add(finder);

            return finder;
        }
    }

    void start()
    {
        //todo sync this method
        maniThread.start();
    }

    void stop()
    {
        synchronized (finders)
        {
            if(!maniThread.isInterrupted())
                maniThread.interrupt();
        }
    }

    public void setCheckTime(long checkTime) {
        if(checkTime<0)
            throw new IllegalArgumentException("time can not be negative");

        this.checkTime = checkTime;
    }
}
