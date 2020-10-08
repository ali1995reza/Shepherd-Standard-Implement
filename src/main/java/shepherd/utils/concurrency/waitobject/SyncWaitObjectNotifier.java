package shepherd.utils.concurrency.waitobject;


import java.util.ArrayDeque;

public class SyncWaitObjectNotifier implements IWaitObjectNotifier {


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

    public SyncWaitObjectNotifier()
    {
        this(DEFAULT_INITIAL_SIZE , DEFAULT_MAXIMUM_QUEUE_SIZE);
    }

    public SyncWaitObjectNotifier(int initialArraySize , int maximumSize)
    {
        waitObjects = new ArrayDeque<>(initialArraySize);
        this.maximumSize = maximumSize;
    }


    private final void waitIfNoSpace()
    {
        ++waiters;
        while (waitObjects.size()>=maximumSize) {
            try {
                waitObjects.wait();
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
            waitObjects.notify();
    }

    public void setMaximumSize(int maximumSize) {

        synchronized (waitObjects) {
            int old = this.maximumSize;
            this.maximumSize = maximumSize;

            if(maximumSize>old)
            {
                notifyIfSpaceNeed();
            }

        }
    }

    @Override
    public void waitForNotify(IWaitObject lock , int threshold) {

        synchronized (waitObjects) {

            waitIfNoSpace();

            if (waitObjects.add(new WaitObjectHolder(lock, threshold)))
                return;
        }

        throw new IllegalStateException("can not add wait object at this time");

    }

    @Override
    public void notifyObjects(final int threshold , int code) {
        WaitObjectHolder current = null;
        synchronized (waitObjects) {
             current = waitObjects.peek();
        }

        if(current==null)return;

        int lastObjectThreshold = current.threshold;

        while (current!=null
                && current.threshold<=threshold
                && current.threshold>=lastObjectThreshold)
        {
            current.object.notifyObject(code);
            lastObjectThreshold = current.threshold;
            synchronized (waitObjects) {
                waitObjects.poll();
                current = waitObjects.peek();
                if(current==null)
                {
                    notifyIfSpaceNeed();
                }
            }
        }
    }

    @Override
    public void removeLastObject() {
        synchronized (waitObjects)
        {
            if(waitObjects.removeLast()!=null)
            {
                notifyIfSpaceNeed();
            }

        }
    }
}
