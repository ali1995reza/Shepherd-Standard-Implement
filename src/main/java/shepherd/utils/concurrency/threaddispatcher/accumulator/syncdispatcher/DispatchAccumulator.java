package shepherd.utils.concurrency.threaddispatcher.accumulator.syncdispatcher;

import java.util.Iterator;

class DispatchAccumulator<T> implements Iterator<T> , Iterable<T> {


    public static class Builder<E>
    {

        private Object _sync = new Object();
        private DispatchAccumulator<E>[] array;
        private int batchSize;
        private int currentPutIndex;
        private int currentPickIndex;
        Builder(int arraySize , int bSize)
        {

            array = new DispatchAccumulator[arraySize];
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

        public DispatchAccumulator<E> build()
        {
            synchronized (_sync)
            {
                DispatchAccumulator<E> b = array[currentPickIndex];
                if(b==null)
                {
                    return new DispatchAccumulator<>(batchSize , this);
                }else
                {
                    array[currentPickIndex++]=null;
                    if(currentPickIndex==array.length)
                        currentPickIndex = 0;
                    return b;
                }
            }
        }

        private void putBack(DispatchAccumulator<E> batch)
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


    private DispatchAccumulator(int s , Builder<T> builder)
    {
        array = new Object[s];
        this.builder = builder;
    }

    public DispatchAccumulator(int s)
    {
        this(s , null);
    }

    boolean accumulate(T d)
    {


        if(d==null)
            throw new NullPointerException();

        if (writeIndex == array.length)
            return false;


        array[writeIndex++] = d;


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
    }


    @Override
    public Iterator<T> iterator() {
        return this;
    }

}
