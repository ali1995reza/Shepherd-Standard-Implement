package shepherd.standard.datachannel.standard.iobatch.unpooled.w;

import java.nio.ByteBuffer;

final class SizeDetectStatic {

    public final static int detectSize(ByteBuffer[] buffers)
    {
        int s = 0;
        for(ByteBuffer buffer:buffers)
        {
            s+=buffer.remaining();
        }

        return s;
    }

    public final static int detectSize(ByteBuffer header , ByteBuffer[] buffers)
    {
        int s = 0;
        s+=header.remaining();
        for(ByteBuffer buffer:buffers)
        {
            s+=buffer.remaining();
        }

        return s;
    }

    public final static void copy(byte priority , ByteBuffer[] buffers , ByteBuffer dest)
    {
        dest.put(priority);
        for(ByteBuffer buffer:buffers)
        {
            dest.put(buffer);
        }
    }

    public final static void copy(byte priority , ByteBuffer header , ByteBuffer[] buffers , ByteBuffer dest)
    {
        dest.put(priority);
        dest.put(header);
        for(ByteBuffer buffer:buffers)
        {
            dest.put(buffer);
        }
    }
}
