package shepherd.standard.datachannel.standard.iobatch.unpooled;

import java.nio.ByteBuffer;

public class RichBufferController implements BufferSizeController {

    private final boolean direct;

    public RichBufferController(boolean direct) {
        this.direct = direct;
    }

    public RichBufferController(){
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
        return allocate(miSize*2);
    }

    @Override
    public ByteBuffer offerReallocate(int currentSize , int minSize, Section section) {
        return null;
    }

    @Override
    public ByteBuffer offerExtend(int currentSize, int howMuch, Section section) {
        return allocate(currentSize + (howMuch*2));
    }

    @Override
    public ByteBuffer mustExtend(int currentSize, int howMuch, Section section) {
        return allocate(currentSize+(howMuch*2));
    }

    @Override
    public String toString() {
        return "RichBufferController{" +
                "direct=" + direct +
                '}';
    }
}
