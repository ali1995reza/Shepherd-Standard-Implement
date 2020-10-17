package shepherd.standard.utils;

import shepherd.standard.assertion.Assertion;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public abstract class TimerThread {


    private final static class CloseThreadException extends RuntimeException{
        private final static CloseThreadException INSTANCE =
                new CloseThreadException();
    }

    private final Thread thread;
    private final BlockingDeque queue;
    private final CountDownLatch latch = new CountDownLatch(1);
    private long waitTime;
    private boolean start;
    private boolean stop;
    private final Map<String , Object> attributes;

    public TimerThread(long w)
    {
        setWaitTime(w);
        thread = new Thread(this::main){
            @Override
            public void interrupt() {
                throw new IllegalStateException("can not interrupt current thread");
            }
        };
        thread.setPriority(Thread.MAX_PRIORITY);
        queue = new LinkedBlockingDeque();
        attributes = new HashMap<>();
    }

    public TimerThread setWaitTime(long waitTime) {
        synchronized (latch) {
            Assertion.ifTrue("wait time cant not be negative", waitTime < 0);
            this.waitTime = waitTime;
            if(start && Thread.currentThread()!=thread)queue.addFirst(thread);
            return this;
        }
    }


    public TimerThread put(Object o)
    {
        synchronized (latch) {
            Assertion.ifFalse("timer not start yet" , start);
            Assertion.ifTrue("timer is stopped" , stop);
            Assertion.ifFalse("can not add to queue now", queue.add(o));
            return this;
        }
    }

    public void start()
    {
        synchronized (latch)
        {
            Assertion.ifTrue("timer is topped" , stop);
            Assertion.ifTrue("timer is started before" , start);
            start = true;
            thread.start();
        }
    }

    public void stop()
    {
        synchronized (latch)
        {
            Assertion.ifFalse("thread not start yet" , start);
            Assertion.ifTrue("thread already stopped" , stop);
            queue.addFirst(latch);
            if(Thread.currentThread()==thread)
            {
                stop = true;
                return;
            }else {
                stop = true;
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    while (latch.getCount()>0);
                }
            }

        }
    }

    public void executeAndStop()
    {
        synchronized (latch)
        {
            Assertion.ifFalse("thread not start yet" , start);
            Assertion.ifTrue("thread already stopped" , stop);
            queue.addLast(latch);

            if(Thread.currentThread()==thread)
            {
                stop = true;
                return;
            }else {
                stop = true;
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    while (latch.getCount()>0);
                }
            }

        }
    }

    private final void main(){
        try {
            init();
        }catch (Throwable e)
        {
            try {
                onException(e);
            }catch (Throwable ex)
            {
                ex.printStackTrace();
            }
        }
        while (true)
        {
            try{
                while (true)
                {
                    loop();
                }
            }catch (CloseThreadException e)
            {
                synchronized (latch)
                {
                    stop = true;
                    break;
                }
            }catch (InterruptedException e){

            }catch (Throwable e){
                try{
                    onException(e);
                }catch (Throwable ex)
                {
                    ex.printStackTrace();
                }
            }
        }

        try{
            onStop();
        }catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    private final void loop()throws InterruptedException{
        Object o = queue.pollFirst(waitTime , TimeUnit.MILLISECONDS);
        if(o==thread){
            loop();
            return;
        }

        if(o == latch)
            throw CloseThreadException.INSTANCE;

        if(o==null)
        {
            onWakeup();
        }else
        {
            onDataAvailable(o);
        }
    }


    protected <T> T getAttribute(String s)
    {
        Assertion.ifFalse("attributes can just accessiable using timer thread" ,
                Thread.currentThread()==thread);

        return (T) attributes.get(s);
    }


    protected <T> T setAttribute(String s , Object o)
    {
        Assertion.ifFalse("attributes can just accessiable using timer thread" ,
                Thread.currentThread()==thread);

        return (T) attributes.put(s , o);
    }

    protected abstract void init();
    protected abstract void onDataAvailable(Object data);
    protected abstract void onWakeup();
    protected abstract void onException(Throwable e);
    protected abstract void onStop();
}
