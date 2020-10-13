package shepherd.standard.cluster.node;

import shepherd.standard.assertion.Assertion;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ObjectLock {

    private final static class MetaHolder{

        public static MetaHolder withAttachment(Object o)
        {
            return new MetaHolder().attach(o);
        }


        private Object attachment;
        private final ReentrantLock lock;

        private MetaHolder() {
            this.lock = new ReentrantLock(false);
        }

        public MetaHolder attach(Object attachment) {
            this.attachment = attachment;
            return this;
        }

        public boolean isCurrentThreadIsAcquisitor()
        {
            return lock.isHeldByCurrentThread();
        }

        public MetaHolder acquire()
        {
            lock.lock();
            return this;
        }

        public MetaHolder release()
        {
            Assertion.ifFalse("current thread not control the lock" ,
                    lock.isHeldByCurrentThread());
            lock.unlock();
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

    public <T> T defineAndAcquire(Object o , Object attach)
    {
        synchronized (objects)
        {
            Assertion.ifTrue("can not define a lock , and lock already exists" ,
                    objects.containsKey(o));

            MetaHolder holder =
                    MetaHolder.withAttachment(attach)
                            .acquire();
            objects.put(
                    o ,
                    holder
            );


            return holder.attachment();
        }
    }

    public <T> T define(Object o , Object attach)
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


            return holder.attachment();
        }
    }

    public <T> T acquire(Object o)
    {
        synchronized (objects)
        {
            MetaHolder holder = objects.get(o);
            Assertion.ifNull("there is no lock for this object" , holder);
            return holder.acquire().attachment();
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
