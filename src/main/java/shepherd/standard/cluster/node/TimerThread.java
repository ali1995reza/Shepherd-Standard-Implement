package shepherd.standard.cluster.node;

import shepherd.standard.assertion.Assertion;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TimerThread {

    private final Thread thread;
    private final BlockingQueue tasks;
    private long roundTime;
    private Consumer roundListener;
    private boolean start;
    private boolean stop;
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final Object stopToken = new Object();

    public TimerThread(Consumer l)
    {
        setRoundListener(l);
        thread = new Thread(this::mainLoop);
        tasks = new LinkedBlockingQueue();
        roundTime = 1000l;
    }

    public TimerThread setRoundTime(long roundTime) {
        this.roundTime = roundTime;
        return this;
    }

    public TimerThread setRoundListener(final Consumer roundListener) {
        Assertion.ifNull("listener can not be null" , roundListener);
        this.roundListener = new Consumer() {
            @Override
            public void accept(Object o) {
                try {
                    roundListener.accept(o);
                }catch (Throwable e)
                {
                    e.printStackTrace();
                }
            }
        };
        return this;
    }

    public synchronized TimerThread scheduleTask(Object o)
    {
        Assertion.ifTrue("thread stopped" , stop);
        Assertion.ifFalse("can not add task" , tasks.add(o));
        return this;
    }

    public synchronized void start()
    {
        Assertion.ifTrue("this thread already stopped" , stop);
        Assertion.ifTrue("this thread already started" , start);
        start = true;
        thread.start();
    }

    public synchronized void stop()
    {
        Assertion.ifFalse("this thread not start yet" , start);
        Assertion.ifTrue("this thread already stopped" , stop);
        stop = true;
        while (!tasks.add(stopToken));
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            while (stopLatch.getCount()>0);
        }
    }

    private void mainLoop()
    {
        while (true)
        {
            try {
                loop();
            } catch (InterruptedException e){
                break;
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        stopLatch.countDown();
    }

    private void loop() throws Exception
    {
        Object o = tasks.poll(roundTime , TimeUnit.MILLISECONDS);
        if(o==stopToken)
            throw new InterruptedException();

        roundListener.accept(o);
    }
}
