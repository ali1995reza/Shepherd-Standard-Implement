package shepherd.standard.datachannel.standard.iobatch.unpooled;

import shepherd.standard.assertion.Assertion;

import java.nio.ByteBuffer;

public class BufferControllerWrapper implements BufferSizeController {

    private BufferSizeController ref;

    BufferControllerWrapper(BufferSizeController ref)
    {
        setRef(ref);
    }

    public BufferControllerWrapper setRef(BufferSizeController ref) {
        Assertion.ifNull("reference can not be null" , ref);
        this.ref = ref;
        return this;
    }

    @Override
    public ByteBuffer mustAllocate(int minSize, Section section) {
        ByteBuffer byteBuffer = ref.mustAllocate(minSize, section);

        if(byteBuffer == null)
            throw new NullPointerException("provided buffer is null");

        if(byteBuffer.capacity()<minSize)
            throw new IllegalStateException("provided buffer has lower size than minimum size");

        return byteBuffer;

    }

    @Override
    public ByteBuffer offerReallocate(int currentSize , int minSize, Section section) {
        ByteBuffer byteBuffer = ref.offerReallocate(currentSize , minSize, section);

        if(byteBuffer == null)return null;

        if(byteBuffer.capacity()<minSize)
            throw new IllegalStateException("provided buffer has lower size than minimum size");

        return byteBuffer;
    }

    @Override
    public ByteBuffer offerExtend(int currentSize, int howMuch, Section section) {
        ByteBuffer byteBuffer = ref.offerExtend(currentSize , howMuch, section);

        if(byteBuffer == null) return null;

        if(byteBuffer.capacity()<currentSize+howMuch)
            throw new IllegalStateException("provided buffer has lower size than minimum size");

        return byteBuffer;
    }

    @Override
    public ByteBuffer mustExtend(int currentSize, int howMuch, Section section) {
        ByteBuffer byteBuffer = ref.mustExtend(currentSize , howMuch , section);

        if(byteBuffer == null)
            throw new NullPointerException("provided buffer is null");

        if(byteBuffer.capacity()<currentSize+howMuch)
            throw new IllegalStateException("provided buffer has lower size than minimum size");

        return byteBuffer;
    }

    @Override
    public String toString() {
        return ref.toString();
    }
}
