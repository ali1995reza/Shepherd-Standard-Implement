package shepherd.standard.datachannel.standard.iobatch.unpooled;

import java.nio.ByteBuffer;
import java.util.Random;

public class RandomBufferController implements BufferSizeController{

    private final boolean direct;
    private final Random random = new Random();

    public RandomBufferController(boolean direct) {
        this.direct = direct;
    }

    public RandomBufferController()
    {
        this(true);
    }

    private ByteBuffer allocate(int size)
    {
        if(direct)
            return ByteBuffer.allocateDirect(size);

        return ByteBuffer.allocate(size);
    }

    @Override
    public ByteBuffer mustAllocate(int miSize, Section section) {
        return allocate(miSize);
    }

    @Override
    public ByteBuffer offerReallocate(int current , int minSize, Section section) {

        if(random.nextBoolean())
            return allocate(minSize);

        return null;
    }

    @Override
    public ByteBuffer offerExtend(int currentSize, int howMuch, Section section) {
        if(random.nextBoolean())
            return allocate(currentSize+howMuch);

        return null;
    }

    @Override
    public ByteBuffer mustExtend(int currentSize, int howMuch, Section section) {
        return allocate(currentSize+howMuch);
    }
}
