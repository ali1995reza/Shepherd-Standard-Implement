package shepherd.utils.collection.objectpool;

public interface ObjectPool<T> {


    T borrow() throws InterruptedException;
    T borrow(long millis) throws InterruptedException;
    T tryBorrow();
    T allocate();

    T tryBorrowOrAllocate();

    boolean free(T buffer);

}
