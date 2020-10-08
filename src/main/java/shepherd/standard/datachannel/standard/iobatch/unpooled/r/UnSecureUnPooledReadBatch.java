package shepherd.standard.datachannel.standard.iobatch.unpooled.r;

import shepherd.standard.datachannel.standard.iobatch.ReadBatch;
import shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSize;
import shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSizeController;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class UnSecureUnPooledReadBatch implements ReadBatch {


    private final PacketParser parser;
    private final BufferSizeController sizeController;
    private long totalReceivedBytes = 0;
    private long totalReceivedPackets = 0;

    public UnSecureUnPooledReadBatch(BufferSizeController sizeController) {
        this.sizeController = sizeController;
        parser = new PacketParser(
                sizeController ,
                BufferSize.KB.toByte() ,
                BufferSizeController.Section.READ_BATCH_AND_CHUNK
        );
    }





    @Override
    public void readFromChannel(SocketChannel channel) throws IOException {

        int read = channel.read(parser.buffer());

        if(read<0)
            throw new IllegalStateException("input closed");

        totalReceivedBytes+=read;

        parser.accumulateBufferedSize(read);
    }

    @Override
    public void readFromBuffer(ByteBuffer buffer) throws IOException {

        int size = buffer.remaining();
        while (buffer.hasRemaining())
        {
            parser.buffer().put(buffer);
        }
        parser.accumulateBufferedSize(size);
    }

    @Override
    public ByteBuffer[] getPacket() throws IOException {
        ByteBuffer[] packet =  parser.getPacket();
        if(packet!=null)++totalReceivedPackets;

        return packet;
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

    public SecureUnPooledReadBatch toSecureMode(SSLEngine engine)
    {
        return new SecureUnPooledReadBatch(
                engine , sizeController
        );
    }
}
