package shepherd.standard.datachannel.standard.iobatch.unpooled;

import java.nio.ByteBuffer;

public class PoorBufferController implements BufferSizeController {


    private final boolean direct;

    public PoorBufferController(boolean direct) {
        this.direct = direct;
    }

    public PoorBufferController(){
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
    public ByteBuffer offerReallocate(int currentSize , int minSize, Section section) {
        return null;
    }

    @Override
    public ByteBuffer offerExtend(int currentSize, int howMuch, Section section) {
        return null;
    }

    @Override
    public ByteBuffer mustExtend(int currentSize, int howMuch, Section section) {
        return allocate(currentSize+howMuch);
    }

    @Override
    public String toString() {
        return "PoorBufferController{" +
                "direct=" + direct +
                '}';
    }
}
