package shepherd.standard.datachannel.standard.iobatch.unpooled;

import java.nio.ByteBuffer;

public class TestSizeController implements BufferSizeController {



    @Override
    public ByteBuffer mustAllocate(int minSize, Section section) {

        if(section==Section.READ_BATCH_AND_CHUNK || section==Section.READ_BATCH||
            section==Section.UNWRAP_AND_CHUNK)
        {
            return ByteBuffer.allocateDirect(1024*1024);
        }

        if(section==Section.WRITE_BATCH) {
            return ByteBuffer.allocateDirect(512 * 1024);
        }

        if(section==Section.WRAP_AND_WRITE)
        {
            return ByteBuffer.allocateDirect(minSize*2);
        }

        //BATCH_AND_WRITE

        return ByteBuffer.allocateDirect(1024*1024);
    }

    @Override
    public ByteBuffer offerReallocate(int currentSize, int minSize, Section section) {
        return null;
    }

    @Override
    public ByteBuffer offerExtend(int currentSize, int howMuch, Section section) {
        return null;
    }

    @Override
    public ByteBuffer mustExtend(int currentSize, int howMuch, Section section) {
        System.out.println("MUST EXTEND : "+howMuch);
        return ByteBuffer.allocateDirect(currentSize+howMuch);
    }
}
