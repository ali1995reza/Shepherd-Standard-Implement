package shepherd.utils.concurrency.lock;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReentrantPriorityLock implements PriorityLock {

    private final static Condition EMPTY_CONDITION = new Condition() {
        @Override
        public void await() throws InterruptedException {

        }

        @Override
        public void awaitUninterruptibly() {

        }

        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            return 0;
        }

        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            return false;
        }

        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            return false;
        }

        @Override
        public void signal() {

        }

        @Override
        public void signalAll() {

        }
    };


    private final static class ConditionHolder{

        private final Condition condition;
        private int size = 0;

        private ConditionHolder(Condition condition) {
            this.condition = condition;
        }
    }

    private final ConditionHolder[] conditions;
    private final ReentrantLock _lock = new ReentrantLock(true);
    private boolean locked = false;
    private Thread locker;
    private final int maxPrio;

    public ReentrantPriorityLock(int size)
    {
        conditions = new ConditionHolder[size+1];
        for(int i=0;i<size+1;i++)
        {
            conditions[i] = new ConditionHolder(_lock.newCondition());
        }
        maxPrio = size;
    }



    private final void doLock(int priority)
    {
        ConditionHolder cond = conditions[priority];

        ++cond.size;

        while (locked)
        {
            try {
                cond.condition.await();
            } catch (InterruptedException e) {
                if(!locked)
                {
                    nextCondition().signal();
                    return;
                }
            }
        }

        locked = true;
        locker = Thread.currentThread();
        --cond.size;
    }


    @Override
    public void lock(int priority)
    {
        _lock.lock();
        try {
            doLock(priority);
        }finally {
            _lock.unlock();
        }
    }


    private Condition nextCondition()
    {
        for(int i=maxPrio;i>0;i--)
        {
            if(conditions[i].size>0) {
                return conditions[i].condition;
            }
        }

        return EMPTY_CONDITION;
    }

    @Override
    public void unlock()
    {
        _lock.lock();
        try {
            nextCondition().signal();
            locker = null;
            locked = false;
        }finally {
            _lock.unlock();
        }
    }

    @Override
    public void lockMaximumPriority() {
        lock(maxPrio);
    }
}
