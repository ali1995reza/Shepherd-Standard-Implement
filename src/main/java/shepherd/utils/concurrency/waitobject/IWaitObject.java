package shepherd.utils.concurrency.waitobject;


import java.util.function.Consumer;

public interface IWaitObject<T> {


    IWaitObject afterNotifiedDo(Consumer<T> listener);
    void notifyObject(int code);


}
