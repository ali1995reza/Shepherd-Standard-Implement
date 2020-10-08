package shepherd.utils.buffer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ExtendableByteBufferInputStream extends InputStream {

    private ByteBuffer[] dataToRead;
    private int index = 0;
    private final boolean TRUE = true;

    public ExtendableByteBufferInputStream()
    {

    }

    @Override
    public int read() throws IOException {

        while (TRUE) {
            if (index == dataToRead.length)
                throw new EOFException("end of buffers");

            if (dataToRead[index].hasRemaining()) break;

            ++index;
        }

        return dataToRead[index].get() & 0xff;
    }

    public void setDataToRead(ByteBuffer[] b)
    {
        dataToRead = b;
        index = 0;
    }
}
