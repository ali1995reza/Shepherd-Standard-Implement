package shepherd.standard.cluster.node;

import shepherd.standard.assertion.Assertion;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class ObjectLock {

    private final static class MetaHolder{

        public static MetaHolder withAttachment(Object o)
        {
            return new MetaHolder().attach(o);
        }


        private Object attachment;
        private final ReentrantLock lock;
        private boolean removed;

        private MetaHolder() {
            this.lock = new ReentrantLock(false);
        }

        public MetaHolder attach(Object attachment) {
            this.attachment = attachment;
            return this;
        }

        public boolean isCurrentThreadAcquisitor()
        {
            return lock.isHeldByCurrentThread();
        }

        public boolean acquire()
        {
            lock.lock();
            if(removed) {
                lock.unlock();
                return false;
            }
            return true;
        }

        public MetaHolder release()
        {
            Assertion.ifFalse("current thread not control the lock" ,
                    lock.isHeldByCurrentThread());
            lock.unlock();
            return this;
        }

        public MetaHolder releaseAndRemove()
        {
            Assertion.ifFalse("current thread not control the lock" ,
                    lock.isHeldByCurrentThread());
            lock.unlock();
            removed = true;
            return this;
        }

        public <T> T attachment()
        {
            Assertion.ifFalse("current thread not control the lock" ,
                    lock.isHeldByCurrentThread());
            return (T)attachment;
        }
    }


    private final Map<Object , MetaHolder> objects;


    public ObjectLock() {
        this.objects = new HashMap<>();
    }

    public boolean defineAndAcquire(Object o , Object attach)
    {
        synchronized (objects)
        {
            Assertion.ifTrue("can not define a lock , and lock already exists" ,
                    objects.containsKey(o));

            MetaHolder holder =
                    MetaHolder.withAttachment(attach);
            objects.put(
                    o ,
                    holder
            );

            return holder.acquire();
        }
    }

    public void define(Object o , Object attach)
    {
        synchronized (objects)
        {
            Assertion.ifTrue("can not define a lock , and lock already exists" ,
                    objects.containsKey(o));

            MetaHolder holder =
                    MetaHolder.withAttachment(attach);

            objects.put(
                    o ,
                    holder
            );
        }
    }

    public boolean acquire(Object o)
    {
        synchronized (objects)
        {
            MetaHolder holder = objects.get(o);
            if(holder==null)return false;
            return holder.acquire();
        }
    }

    public void release(Object o)
    {
        synchronized (objects)
        {
            MetaHolder holder = objects.get(o);
            Assertion.ifNull("there is no lock for this object" , holder);
            holder.release();
        }
    }

    public void releaseAndRemove(Object o)
    {
        synchronized (objects)
        {
            MetaHolder holder = objects.remove(o);
            Assertion.ifNull("there is no lock for this object" , holder);
            holder.releaseAndRemove();
        }
    }

    public void attach(Object o , Object attachment)
    {
        synchronized (objects)
        {
            MetaHolder holder = objects.get(o);
            Assertion.ifNull("there is no lock for this object" , holder);
            holder.attach(attachment);
        }
    }

    public <T> T attachment(Object o)
    {
        synchronized (objects)
        {
            MetaHolder holder = objects.get(o);
            Assertion.ifNull("there is no lock for this object" , holder);
            return holder.attachment();
        }
    }
}
