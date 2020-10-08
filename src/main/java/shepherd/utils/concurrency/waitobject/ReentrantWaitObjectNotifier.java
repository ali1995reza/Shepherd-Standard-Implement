package shepherd.utils.concurrency.waitobject;

import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReentrantWaitObjectNotifier implements IWaitObjectNotifier {


    private static class WaitObjectHolder
    {
        IWaitObject object;
        int threshold;
        WaitObjectHolder(IWaitObject o , int t)
        {
            object = o;
            threshold = t;
        }
    }

    private static final int DEFAULT_INITIAL_SIZE = 5000;

    private static final int DEFAULT_MAXIMUM_QUEUE_SIZE = 1000000;




    private ArrayDeque<WaitObjectHolder> waitObjects;
    private int maximumSize;
    private int waiters = 0;
    private final ReentrantLock lock;
    private final Condition needSpaceCond;

    public ReentrantWaitObjectNotifier()
    {
        this(DEFAULT_INITIAL_SIZE , DEFAULT_MAXIMUM_QUEUE_SIZE);
    }

    public ReentrantWaitObjectNotifier(int initialArraySize , int maximumSize)
    {
        waitObjects = new ArrayDeque<>(initialArraySize);
        this.maximumSize = maximumSize;
        lock = new ReentrantLock(true);
        needSpaceCond = lock.newCondition();
    }


    private final void waitIfNoSpace()
    {
        ++waiters;
        while (waitObjects.size()>=maximumSize) {
            try {
                needSpaceCond.await();
            } catch (InterruptedException e) {
                --waiters;
                throw new IllegalStateException(e);
            }
        }
        --waiters;
    }

    private final void notifyIfSpaceNeed()
    {
        if(waiters>0)
            needSpaceCond.signal();
    }

    public void setMaximumSize(int maximumSize) {

        lock.lock();
        try  {
            int old = this.maximumSize;
            this.maximumSize = maximumSize;

            if(maximumSize>old)
            {
                notifyIfSpaceNeed();
            }

        }finally {
            lock.unlock();
        }

    }

    @Override
    public void waitForNotify(IWaitObject waitObject , int threshold) {

        lock.lock();

        try{

            waitIfNoSpace();

            if (waitObjects.add(new WaitObjectHolder(waitObject, threshold)))
                return;


        }finally {
            lock.unlock();
        }

        throw new IllegalStateException("can not add wait object at this time");

    }

    @Override
    public void notifyObjects(final int threshold , int code) {
        WaitObjectHolder current = null;
        lock.lock();
        try {
            current = waitObjects.peek();
        }finally {
            lock.unlock();
        }

        if(current==null)return;

        int lastObjectThreshold = current.threshold;

        while (current!=null
                && current.threshold<=threshold
                && current.threshold>=lastObjectThreshold)
        {
            current.object.notifyObject(code);
            lastObjectThreshold = current.threshold;
            lock.lock();
            try {
                waitObjects.poll();
                current = waitObjects.peek();
                if(current==null)
                {
                    notifyIfSpaceNeed();
                }
            }finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void removeLastObject() {
        lock.lock();
        try {
            if(waitObjects.removeLast()!=null)
            {
                notifyIfSpaceNeed();
            }
        }finally {
            lock.unlock();
        }
    }
}
