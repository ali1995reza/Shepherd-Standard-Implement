package shepherd.standard.asynchrounous;

import shepherd.api.asynchronous.AsynchronousResultListener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SynchronizerListener<T> implements AsynchronousResultListener<T> {

    private T result;
    private final CountDownLatch latch = new CountDownLatch(1);


    @Override
    public void onUpdated(T result) {

    }

    @Override
    public void onCompleted(T result) {
        this.result = result;
        latch.countDown();
    }

    public T sync() throws InterruptedException {

        latch.await();
        return result;
    }

    public T sync(long milli) throws InterruptedException {
        latch.await(milli , TimeUnit.MILLISECONDS);
        return result;
    }

    public T sync(long l , TimeUnit unit) throws InterruptedException {
        latch.await(l , unit);
        return result;
    }

    public T syncUninterruptible()
    {
        try {
            latch.await();
            return result;
        } catch (InterruptedException e) {

            while (latch.getCount()>0);
            return result;
        }
    }
}
