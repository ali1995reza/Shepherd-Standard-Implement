package shepherd.utils.collection;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

public class ManualSyncArrayBlockingDeque<T> implements BlockingDeque<T> {





    private final ArrayDeque<T> arrayDeque;
    private int waiting = 0;
    private final Object _sync = new Object();

    public ManualSyncArrayBlockingDeque(int initialCapacity)
    {
        arrayDeque = new ArrayDeque<>(initialCapacity);
    }

    public ManualSyncArrayBlockingDeque()
    {
        arrayDeque = new ArrayDeque<>();
    }






    @Override
    public void addFirst(T t) {

        arrayDeque.addFirst(t);
        notifyIfAnyOneWaitForTake();
    }

    public void syncAddFirst(T t)
    {
        synchronized (_sync)
        {
            addFirst(t);
        }
    }

    @Override
    public void addLast(T t) {

        arrayDeque.addLast(t);
        notifyIfAnyOneWaitForTake();
    }

    public void syncAddLast(T t)
    {
        synchronized (_sync)
        {
            addLast(t);
        }
    }

    @Override
    public boolean offerFirst(T t) {
        if(arrayDeque.offerFirst(t))
        {
            notifyIfAnyOneWaitForTake();
            return true;
        }
        return false;
    }


    @Override
    public boolean offerLast(T t) {

        if(arrayDeque.offerLast(t))
        {
            notifyIfAnyOneWaitForTake();
            return true;
        }
        return false;

    }


    @Override
    public T removeFirst() {

        return arrayDeque.removeFirst();
    }

    @Override
    public T removeLast() {

        return arrayDeque.removeLast();
    }

    @Override
    public T pollFirst() {

        return arrayDeque.pollFirst();
    }

    public T syncPollFirst()
    {
        synchronized (_sync)
        {
            return arrayDeque.pollFirst();
        }
    }

    @Override
    public T pollLast() {

        return arrayDeque.pollLast();
    }

    public T syncPollLast()
    {
        synchronized (_sync)
        {
            return arrayDeque.pollLast();
        }
    }

    @Override
    public T getFirst() {

        return arrayDeque.getFirst();
    }

    @Override
    public T getLast() {

        return arrayDeque.getLast();
    }

    @Override
    public T peekFirst() {

        return arrayDeque.peekFirst();
    }

    @Override
    public T peekLast() {

        return arrayDeque.peekLast();
    }

    @Override
    public void putFirst(T t) throws InterruptedException {

        arrayDeque.addFirst(t);
        notifyIfAnyOneWaitForTake();
    }

    @Override
    public void putLast(T t) throws InterruptedException {

        arrayDeque.addLast(t);
        notifyIfAnyOneWaitForTake();
    }

    @Override
    public boolean offerFirst(T t, long timeout, TimeUnit unit) throws InterruptedException {
        addFirst(t);
        return true;
    }

    @Override
    public boolean offerLast(T t, long timeout, TimeUnit unit) throws InterruptedException {
        addLast(t);
        return true;
    }

    @Override
    public T takeFirst() throws InterruptedException {

        T t = arrayDeque.pollFirst();
        while (t == null)
        {
            waitForTake();
            t = arrayDeque.pollFirst();


        }

        return t;
    }

    public T syncTakeFirst() throws InterruptedException{

        synchronized (_sync)
        {
            return takeFirst();
        }
    }

    @Override
    public T takeLast() throws InterruptedException {
        T t = arrayDeque.pollLast();
        while (t == null)
        {
            waitForTake();
            t = arrayDeque.pollLast();
        }


        return t;
    }

    public T syncTakeLast() throws InterruptedException{

        synchronized (_sync)
        {
            return takeLast();
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
        T t = arrayDeque.pollFirst();
        while (t == null)
        {
            waitForTake(timeout , unit);

            return arrayDeque.pollFirst();
        }


        return t;
    }

    public T syncPollFirst(long timeOur , TimeUnit unit) throws InterruptedException{
        synchronized (_sync)
        {
            return pollFirst(timeOur, unit);
        }
    }

    @Override
    public T pollLast(long timeout, TimeUnit unit) throws InterruptedException {

        T t = arrayDeque.pollLast();
        while (t == null)
        {
            waitForTake(timeout , unit);

            return arrayDeque.pollLast();
        }


        return t;
    }

    public T syncPollLast(long timeOur , TimeUnit unit) throws InterruptedException{
        synchronized (_sync)
        {
            return pollLast(timeOur, unit);
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
        boolean success = arrayDeque.add(t);
        if(success) {
            notifyIfAnyOneWaitForTake();
        }

        return success;
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

    public T syncTake() throws InterruptedException{

        synchronized (_sync)
        {
            return takeFirst();
        }
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


    public final Object syncObject()
    {
        return _sync;
    }


}
