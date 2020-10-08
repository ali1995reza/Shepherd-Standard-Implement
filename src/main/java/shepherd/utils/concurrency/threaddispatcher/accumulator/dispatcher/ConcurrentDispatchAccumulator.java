package shepherd.utils.concurrency.threaddispatcher.accumulator.dispatcher;

import java.util.Iterator;

class ConcurrentDispatchAccumulator<T> implements Iterator<T> , Iterable<T> {


    public static class Builder<E>
    {

        private Object _sync = new Object();
        private ConcurrentDispatchAccumulator<E>[] array;
        private int batchSize;
        private int currentPutIndex;
        private int currentPickIndex;
        Builder(int arraySize , int bSize)
        {

            array = new ConcurrentDispatchAccumulator[arraySize];
            this.batchSize = bSize;
        }

        Builder(int bSize)
        {
            this(100 , bSize);
        }

        Builder()
        {
            this(100 , 100);
        }

        public ConcurrentDispatchAccumulator<E> build()
        {
            synchronized (_sync)
            {
                ConcurrentDispatchAccumulator<E> b = array[currentPickIndex];
                if(b==null)
                {
                    return new ConcurrentDispatchAccumulator<>(batchSize , this);
                }else
                {
                    array[currentPickIndex++]=null;
                    if(currentPickIndex==array.length)
                        currentPickIndex = 0;
                    return b;
                }
            }
        }

        private void putBack(ConcurrentDispatchAccumulator<E> batch)
        {
            synchronized (_sync)
            {
                array[currentPutIndex++] = batch;
                if(currentPutIndex==array.length)
                    currentPutIndex = 0;
            }
        }

    }




















    private Object[] array;
    private int writeIndex = 0;
    private int readIndex = 0;
    private Builder<T> builder;
    private boolean consumed = false;


    private ConcurrentDispatchAccumulator(int s , Builder<T> builder)
    {
        array = new Object[s];
        this.builder = builder;
    }

    public ConcurrentDispatchAccumulator(int s)
    {
        this(s , null);
    }

    boolean accumulate(T d)
    {


        if(d==null)
            throw new NullPointerException();

        synchronized (array) {

            if (writeIndex == array.length)
                return false;

            if(consumed)
                return false;


            array[writeIndex++] = d;

        }
        return true;
    }


    @Override
    public boolean hasNext() {

        if(readIndex<writeIndex)
        {
            return true;

        }else
        {
            if(builder!=null) {
                reset();
                builder.putBack(this);
            }
            return false;
        }

    }

    @Override
    public T next() {
        T d = (T)array[readIndex];
        array[readIndex++] = null;

        return d;
    }


    private void reset()
    {
        readIndex = 0;
        writeIndex = 0;
        consumed = false;
    }


    @Override
    public Iterator<T> iterator() {
        synchronized (array) {
            consumed = true;
            return this;
        }
    }


    @Override
    public String toString() {
        return hashCode()+" @ w : "+writeIndex+" , r : "+readIndex;
    }
}
