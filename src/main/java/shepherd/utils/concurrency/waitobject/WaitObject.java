package shepherd.utils.concurrency.waitobject;

import java.util.function.Consumer;

public class WaitObject<T> implements IWaitObject<T> {

    private Consumer<T> listener;
    private int releaseNotifyNeed;
    private T data;

    public WaitObject(int rnn , T data)
    {
        releaseNotifyNeed = rnn;
        this.data = data;
    }


    @Override
    public IWaitObject afterNotifiedDo(Consumer<T> l) {
        listener = l;
        return this;
    }

    @Override
    public void notifyObject(int code) {
        synchronized (this)
        {
            if(--releaseNotifyNeed!=0) return;
        }
        if(listener!=null)
            listener.accept(data);
    }
}
