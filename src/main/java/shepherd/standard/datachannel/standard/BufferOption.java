package shepherd.standard.datachannel.standard;

import shepherd.standard.assertion.Assertion;

import java.nio.ByteBuffer;

public final class BufferOption {

    public final static ByteBuffer allocate(BufferOption option)
    {
        if(option.direct)
        {
            return ByteBuffer.allocateDirect(option.size);
        }

        return ByteBuffer.allocate(option.size);
    }



    private final boolean direct;
    private final int size;


    public BufferOption(boolean direct, int size) {
        this.direct = direct;
        this.size = size;
        Assertion.ifFalse("buffer size at least must be 1024" ,
                this.size>=1024);
    }

    public boolean isDirect() {
        return direct;
    }

    public int size() {
        return size;
    }

    public boolean hasEqualOption(ByteBuffer buffer)
    {
        return ((buffer.isDirect()&&direct) ||
                (!buffer.isDirect()&&!direct)) &&
                buffer.capacity() == size;
    }

    @Override
    public String toString() {
        return "BufferOption{" +
                "direct=" + direct +
                ", size=" + size +
                '}';
    }
}
