package shepherd.utils.common;

public class Reference<T> {

    public final static Reference nullReference = new Reference();

    private T data;

    public Reference(T d)
    {
        data = d;
    }

    private Reference(){}


    public T data() {
        return data;
    }

}
