package shepherd.utils.buffer;

import java.io.IOException;
import java.io.InputStream;

public class ByteBufferArrayInputStream extends InputStream {

    private ByteBufferArray array;

    public ByteBufferArrayInputStream(ByteBufferArray a)
    {
        array = a;
    }

    @Override
    public int read() throws IOException {
        try{
            return array.get() & 0xff;
        }catch (Throwable e)
        {
            throw new IOException(e);
        }
    }
}
