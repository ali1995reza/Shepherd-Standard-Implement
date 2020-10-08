package shepherd.standard.datachannel.standard.iobatch.unpooled;

import shepherd.api.logger.Logger;
import shepherd.api.logger.LoggerFactory;

import java.nio.ByteBuffer;

public class LoggerBufferController implements BufferSizeController {

    public final static BufferSizeController log(BufferSizeController ref)
    {
        return new LoggerBufferController(ref);
    }

    private final BufferSizeController ref;
    private final Logger logger;

    public LoggerBufferController(BufferSizeController ref) {
        this.ref = ref;
        logger = LoggerFactory.factory().getLogger(this);
    }


    @Override
    public ByteBuffer mustAllocate(int miSize, Section section) {
        logger.information("mustAllocate({},{})" , miSize , section);
        return ref.mustAllocate(miSize, section);
    }

    @Override
    public ByteBuffer offerReallocate(int currentSize, int minSize, Section section) {
        logger.information("offerReallocate({},{},{})" , currentSize , minSize , section);
        return ref.offerReallocate(currentSize, minSize, section);
    }

    @Override
    public ByteBuffer offerExtend(int currentSize, int howMuch, Section section) {
        logger.information("offerExtend({},{},{})" , currentSize , howMuch , section);
        return ref.offerExtend(currentSize, howMuch, section);
    }

    @Override
    public ByteBuffer mustExtend(int currentSize, int howMuch, Section section) {
        logger.information("mustExtend({},{},{})" , currentSize , howMuch , section);
        return ref.mustExtend(currentSize, howMuch, section);
    }

    @Override
    public String toString() {
        return "SizeLog";
    }
}
