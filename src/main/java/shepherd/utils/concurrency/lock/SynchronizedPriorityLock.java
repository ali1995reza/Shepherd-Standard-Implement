package shepherd.utils.concurrency.lock;

public class SynchronizedPriorityLock implements PriorityLock {
    private final int[] lockCount;
    private final Object _lock = new Object();
    private int currentPriority = -1;
    private boolean locked = false;
    private final int maxPrio;
    private Thread locker;

    public SynchronizedPriorityLock(int size)
    {
        lockCount = new int[size+1];
        maxPrio = size;
    }



    private final void doLock(int priority)
    {
        if (priority > currentPriority)
            currentPriority = priority;

        while (priority < currentPriority || locked) {

            try {
                _lock.wait();
            } catch (InterruptedException e) {
                --lockCount[priority];
                fixCurrentPriority();
                _lock.notifyAll();
                throw new IllegalStateException(e);
            }
        }
        locked = true;
        locker = Thread.currentThread();
        --lockCount[priority];
    }


    @Override
    public void lock(int priority)
    {
        synchronized (_lock) {
            doLock(priority);
        }
    }



    private void fixCurrentPriority()
    {

        locked = false;
        for(int i=currentPriority;i>=0;i--)
        {
            if(lockCount[i]>0) {
                currentPriority = i;
                return;
            }
        }
        currentPriority = -1;
    }

    @Override
    public void unlock()
    {
        synchronized (_lock) {
            fixCurrentPriority();
            locker = null;
            _lock.notifyAll();
        }
    }

    @Override
    public void lockMaximumPriority() {
        lock(maxPrio);
    }
}

