package shepherd.utils.collection;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

public class ArrayBlockingDeque<T> implements BlockingDeque<T> {






    private final ArrayDeque<T> arrayDeque;
    private int waiting = 0;
    private final Object _sync;




    public ArrayBlockingDeque(int initialCapacity)
    {


        arrayDeque = new ArrayDeque<>(initialCapacity);
        _sync = new Object();
    }

    public ArrayBlockingDeque()
    {

        arrayDeque = new ArrayDeque<>();
        _sync = new Object();
    }









    @Override
    public void addFirst(T t) {
        synchronized (_sync)
        {
            arrayDeque.addFirst(t);
            notifyIfAnyOneWaitForTake();
        }
    }

    @Override
    public void addLast(T t) {
        synchronized (_sync)
        {
            arrayDeque.addLast(t);
            notifyIfAnyOneWaitForTake();
        }
    }

    @Override
    public boolean offerFirst(T t) {
        synchronized (_sync)
        {
            if(arrayDeque.offerFirst(t))
            {
                notifyIfAnyOneWaitForTake();
                return true;
            }
            return false;
        }
    }


    @Override
    public boolean offerLast(T t) {
        synchronized (_sync)
        {
            if(arrayDeque.offerLast(t))
            {
                notifyIfAnyOneWaitForTake();
                return true;
            }
            return false;
        }
    }


    @Override
    public T removeFirst() {
        synchronized (_sync)
        {
            return arrayDeque.removeFirst();
        }
    }

    @Override
    public T removeLast() {
        synchronized (_sync)
        {
            return arrayDeque.removeLast();
        }
    }

    @Override
    public T pollFirst() {
        synchronized (_sync)
        {
            return arrayDeque.pollFirst();
        }
    }

    @Override
    public T pollLast() {
        synchronized (_sync)
        {
            return arrayDeque.pollLast();
        }
    }

    @Override
    public T getFirst() {
        synchronized (_sync)
        {
            return arrayDeque.getFirst();
        }
    }

    @Override
    public T getLast() {
        synchronized (_sync)
        {
            return arrayDeque.getLast();
        }
    }

    @Override
    public T peekFirst() {
        synchronized (_sync)
        {
            return arrayDeque.peekFirst();
        }
    }

    @Override
    public T peekLast() {
        synchronized (_sync)
        {
            return arrayDeque.peekLast();
        }
    }

    @Override
    public void putFirst(T t) throws InterruptedException {
        synchronized (_sync)
        {
            arrayDeque.addFirst(t);
        }
    }

    @Override
    public void putLast(T t) throws InterruptedException {
        synchronized (_sync)
        {
            arrayDeque.addLast(t);
        }
    }

    @Override
    public boolean offerFirst(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public boolean offerLast(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public T takeFirst() throws InterruptedException {
        synchronized (_sync)
        {
            T t = arrayDeque.pollFirst();
            while (t == null)
            {
                waitForTake();
                t = arrayDeque.pollFirst();


            }

            return t;
        }
    }

    @Override
    public T takeLast() throws InterruptedException {
        synchronized (_sync)
        {
            T t = arrayDeque.pollLast();
            while (t == null)
            {
                waitForTake();
                t = arrayDeque.pollLast();
            }


            return t;
        }
    }

    private void waitForTake() throws InterruptedException
    {
        ++waiting;
        try {
            _sync.wait();
        }catch (InterruptedException e)
        {
            --waiting;
            throw e;
        }
        --waiting;
    }


    private void waitForTake(long timeOut , TimeUnit unit) throws InterruptedException
    {
        ++waiting;
        try {
            _sync.wait(unit.toMillis(timeOut));
        }catch (InterruptedException e)
        {
            --waiting;
            throw e;
        }
        --waiting;
    }


    private void notifyIfAnyOneWaitForTake()
    {
        if(waiting>0)
            _sync.notify();
    }

    @Override
    public T pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
        synchronized (_sync)
        {
            T t = arrayDeque.pollFirst();
            while (t == null)
            {
                waitForTake(timeout , unit);

                return arrayDeque.pollFirst();
            }


            return t;


        }
    }

    @Override
    public T pollLast(long timeout, TimeUnit unit) throws InterruptedException {
        synchronized (_sync)
        {
            T t = arrayDeque.pollLast();
            while (t == null)
            {
                waitForTake(timeout , unit);

                return arrayDeque.pollLast();
            }


            return t;


        }
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
        return false;
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
        return false;
    }

    @Override
    public boolean add(T t) {
        synchronized (_sync){
            boolean success = arrayDeque.add(t);
            if(success) {
                notifyIfAnyOneWaitForTake();
            }

            return success;
        }
    }

    @Override
    public boolean offer(T t) {
        return false;
    }

    @Override
    public void put(T t) throws InterruptedException {

    }

    @Override
    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public T remove() {
        return null;
    }

    @Override
    public T poll() {
        return null;
    }

    @Override
    public T take() throws InterruptedException {
        return takeFirst();
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return 0;
    }

    @Override
    public T element() {
        return null;
    }

    @Override
    public T peek() {
        return null;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        return 0;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Iterator<T> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return null;
    }

    @Override
    public Iterator<T> descendingIterator() {
        return null;
    }

    @Override
    public void push(T t) {

    }

    @Override
    public T pop() {
        return null;
    }

}
