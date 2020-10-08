package shepherd.utils.concurrency.waitobject;

public interface IWaitObjectNotifier<T> {



    void waitForNotify(IWaitObject lock, int threshold);
    void notifyObjects(int threshold, int code);
    void removeLastObject();

}
