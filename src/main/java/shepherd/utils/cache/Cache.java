package shepherd.utils.cache;

import java.util.Iterator;

public class Cache<T> implements Iterable<T> {

    private Object[] cachedData;
    private int currentPoint = 0;
    private final int size;


    public Cache(int size)
    {

        cachedData = new Object[size];
        this.size = size;
    }

    public void cache(T data)
    {
        if(data==null)
            throw new NullPointerException("data is null");


        cachedData[currentPoint++] = data;
        if(currentPoint==size)
            currentPoint = 0;
    }





    @Override
    public Iterator<T> iterator() {
        Iterator<T> iterator = new Iterator<T>() {
            private final int currentPointAtThisTime = currentPoint;
            private int pointer = cachedData[currentPoint]==null?0:currentPoint;
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                int index = pointer;
                if(++pointer==size)
                    pointer=0;
                return (T)cachedData[index];
            }
        };
        return iterator;
    }
}
