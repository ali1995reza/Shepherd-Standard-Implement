package shepherd.standard.datachannel.standard.iobatch.unpooled;

import java.nio.ByteBuffer;

public class FixedLenBufferController implements BufferSizeController {



    private final int size;

    public FixedLenBufferController(int size) {
        this.size = size;
    }

    @Override
    public ByteBuffer mustAllocate(int miSize, Section section) {

        if(miSize>size)
            return null;

        return ByteBuffer.allocateDirect(size);
    }

    @Override
    public ByteBuffer offerReallocate(int currentSize, int minSize, Section section) {

        if(currentSize==size)
            return null;

        if(minSize>size)
            return null;

        return ByteBuffer.allocateDirect(size);
    }

    @Override
    public ByteBuffer offerExtend(int currentSize, int howMuch, Section section) {
        return null;
    }

    @Override
    public ByteBuffer mustExtend(int currentSize, int howMuch, Section section) {
        return null;
    }


    @Override
    public String toString() {
        return "FixedLenBufferController{" +
                "size=" + size +
                '}';
    }
}
