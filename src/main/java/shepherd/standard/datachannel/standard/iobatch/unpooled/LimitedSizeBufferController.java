package shepherd.standard.datachannel.standard.iobatch.unpooled;

import java.nio.ByteBuffer;

public class LimitedSizeBufferController implements BufferSizeController {


    private final int limit;
    private final int least;
    private final boolean direct;

    public LimitedSizeBufferController(int least , int limit, boolean direct) {
        this.least = least;
        this.limit = limit;
        this.direct = direct;
    }

    public LimitedSizeBufferController(int least , int limit)
    {
        this(least , limit , true);
    }

    private ByteBuffer allocate(int size)
    {
        if(direct)
            return ByteBuffer.allocateDirect(size);

        return ByteBuffer.allocate(size);
    }


    @Override
    public ByteBuffer mustAllocate(int minSize, Section section) {
        if(minSize<least)
            minSize = least;
        return allocate(minSize);
    }

    @Override
    public ByteBuffer offerReallocate(int currentSize, int minSize, Section section) {
        return null;
    }

    @Override
    public ByteBuffer offerExtend(int currentSize, int howMuch, Section section) {
        if(currentSize+howMuch>limit)
            return null;

        return allocate(currentSize+howMuch);
    }

    @Override
    public ByteBuffer mustExtend(int currentSize, int howMuch, Section section) {
        return allocate(currentSize+(howMuch*10));
    }
}
