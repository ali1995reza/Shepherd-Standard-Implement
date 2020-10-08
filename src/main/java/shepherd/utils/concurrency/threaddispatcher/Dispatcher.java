package shepherd.utils.concurrency.threaddispatcher;

import shepherd.utils.concurrency.threaddispatcher.exceptions.DispatcherException;

import java.util.List;
import java.util.function.Consumer;

public interface Dispatcher<T> {


    public void start();
    public void setNumberOfHandlerThreads(int threads) throws DispatcherException;
    public boolean tryDispatch(T data);
    public void dispatch(T data) throws InterruptedException;
    public List<T>  terminate();
    public void terminateAndWaitToFinish();
    public void setConsumer(Consumer<T> consumer);
    public int numberOfThreads();
    public boolean isActive();
}
