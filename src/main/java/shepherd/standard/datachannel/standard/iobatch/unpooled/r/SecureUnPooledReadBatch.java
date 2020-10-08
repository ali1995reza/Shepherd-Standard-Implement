package shepherd.standard.datachannel.standard.iobatch.unpooled.r;

import shepherd.standard.datachannel.standard.iobatch.ReadBatch;
import shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSizeController;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class SecureUnPooledReadBatch implements ReadBatch {



    private final SSLEngine sslEngine;
    private final PacketParser parser;
    private ByteBuffer readBuffer;
    private int actualRead = 0;
    private final BufferSizeController sizeController;
    private final int MINIMUM_READ_BUFFER_SIZE;

    private long totalReceivedBytes = 0;
    private long totalReceivedPackets = 0;

    public SecureUnPooledReadBatch(SSLEngine sslEngine , BufferSizeController sizeController) {
        this.sslEngine = sslEngine;
        this.parser = new PacketParser(
                sizeController ,
                this.sslEngine.getSession().getPacketBufferSize() ,
                BufferSizeController.Section.UNWRAP_AND_CHUNK
        );

        MINIMUM_READ_BUFFER_SIZE = this.sslEngine.getSession().getPacketBufferSize();

        this.sizeController = sizeController;
        allocateReadBuffer();
    }

    private void allocateReadBuffer()
    {
        readBuffer = sizeController.mustAllocate(
                MINIMUM_READ_BUFFER_SIZE ,
                BufferSizeController.Section.READ_BATCH
        );
    }

    private ByteBuffer offerReallocate()
    {
        return sizeController.offerReallocate(
                readBuffer.capacity() ,
                MINIMUM_READ_BUFFER_SIZE ,
                BufferSizeController.Section.READ_BATCH
        );
    }


    private final SSLEngineResult decrypt() throws IOException
    {

        SSLEngineResult result = sslEngine.unwrap(readBuffer , parser.buffer());

        if(result.getStatus() == SSLEngineResult.Status.CLOSED)
            throw new IOException("ssl engine closed");

        return result;
    }



    @Override
    public void readFromChannel(SocketChannel channel) throws IOException {
        int read = channel.read(readBuffer);
        if(read<0)
            throw new IllegalStateException("input closed");

        totalReceivedBytes+=read;
    }

    @Override
    public void readFromBuffer(ByteBuffer buffer) throws IOException {
        throw new IllegalStateException("not implemented yet");
    }

    @Override
    public ByteBuffer[] getPacket() throws IOException {

        ByteBuffer[] packet = parser.getPacket();
        if(packet!=null)
        {
            ++totalReceivedPackets;
            return packet;
        }


        final int position = readBuffer.position();
        readBuffer.limit(position).position(actualRead);

        SSLEngineResult result = decrypt();

        if(result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW)
        {
            //so need to read more please !
            if(actualRead==0 && position==readBuffer.capacity())
                throw new IllegalStateException("can not buffer data to decrypt");



            if(position==readBuffer.capacity()) {

                ByteBuffer newBuffer = offerReallocate();

                if(newBuffer==null)
                {
                    readBuffer.compact();
                    actualRead = 0;
                }else {
                    newBuffer.clear();
                    newBuffer.put(readBuffer);
                    actualRead = 0;
                    readBuffer = newBuffer;
                }

            }else {
                //so just reset the buffer please !
                readBuffer.limit(readBuffer.capacity());
                readBuffer.position(position);
            }
            return null;
        }


        //reset buffer state

        readBuffer.limit(readBuffer.capacity());
        readBuffer.position(position);

        if(result.getStatus()== SSLEngineResult.Status.OK)
        {

            actualRead+=result.bytesConsumed();
            parser.accumulateBufferedSize(result.bytesProduced());

            if(actualRead==position)
            {
                //so we can clean buffer here !
                //do it

                ByteBuffer newBuffer = offerReallocate();
                actualRead = 0;

                if(newBuffer!=null)
                {
                    readBuffer = newBuffer;
                }

                readBuffer.clear();

            }

            return getPacket();
        }else {
            //BUFFER_OVERFLOW
            //allocate a new buffer by packet parser and again try to get packet !
            parser.newBuffer();
            return getPacket();
        }
    }

    @Override
    public void setSecure(SSLEngine engine) {

    }

    @Override
    public long totalReceivedBytes() {
        return totalReceivedBytes;
    }

    @Override
    public long totalReceivedPackets() {
        return totalReceivedPackets;
    }
}
