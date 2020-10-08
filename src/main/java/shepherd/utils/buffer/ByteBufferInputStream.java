package shepherd.utils.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer b)
    {
        if(b == null)
            throw new NullPointerException("buffer is null");

        buffer = b;
    }

    @Override
    public int read() throws IOException {
        return buffer.get() & 0xff;
    }
}
