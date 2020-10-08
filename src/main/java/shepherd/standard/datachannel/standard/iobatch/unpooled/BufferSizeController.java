package shepherd.standard.datachannel.standard.iobatch.unpooled;

import java.nio.ByteBuffer;

public interface BufferSizeController {


    enum Section{
        WRITE_BATCH(1), READ_BATCH_AND_CHUNK(2), WRITE_AND_BATCH(3), READ_BATCH(4),
        WRAP_AND_WRITE(5), UNWRAP_AND_CHUNK(6);


        public final int code;

        Section(int code) {
            this.code = code;
        }

        public boolean is(Section other)
        {
            return this == other;
        }
    }


    ByteBuffer mustAllocate(int miSize, Section section);
    ByteBuffer offerReallocate(int currentSize, int minSize, Section section);
    ByteBuffer offerExtend(int currentSize, int howMuch, Section section);
    ByteBuffer mustExtend(int currentSize, int howMuch, Section section);

}
