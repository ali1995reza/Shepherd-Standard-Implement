package shepherd.standard.datachannel.standard.iobatch.unpooled.r;

import shepherd.standard.datachannel.standard.iobatch.ReadBatch;
import shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSizeController;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class UnPooledReadBatch implements ReadBatch {


    private ReadBatch readBatch;

    public UnPooledReadBatch(BufferSizeController sizeController)
    {
        readBatch = new UnSecureUnPooledReadBatch(sizeController);
    }


    @Override
    public void readFromChannel(SocketChannel channel) throws IOException {
        readBatch.readFromChannel(channel);
    }

    @Override
    public void readFromBuffer(ByteBuffer buffer) throws IOException {
        readBatch.readFromBuffer(buffer);
    }

    @Override
    public ByteBuffer[] getPacket() throws IOException {
        return readBatch.getPacket();
    }

    @Override
    public synchronized void setSecure(SSLEngine engine) {

        if(readBatch instanceof UnSecureUnPooledReadBatch) {
            readBatch = ((UnSecureUnPooledReadBatch) readBatch).toSecureMode(engine);
        }
    }

    @Override
    public long totalReceivedBytes() {
        return readBatch.totalReceivedBytes();
    }

    @Override
    public long totalReceivedPackets() {
        return readBatch.totalReceivedPackets();
    }

}
