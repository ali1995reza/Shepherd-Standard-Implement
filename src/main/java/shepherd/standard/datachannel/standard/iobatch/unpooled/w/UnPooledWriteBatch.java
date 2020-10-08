package shepherd.standard.datachannel.standard.iobatch.unpooled.w;

import shepherd.standard.datachannel.standard.iobatch.WriteBatch;
import shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSizeController;
import shepherd.utils.transport.nio.model.IoState;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class UnPooledWriteBatch implements WriteBatch {

    private WriteBatch writeBatch;

    public UnPooledWriteBatch(BufferSizeController controller)
    {
        writeBatch = new UnSecureUnPooledWriteBatch(controller);
    }


    @Override
    public void append(ByteBuffer[] data, byte priority, IoState state) {
        writeBatch.append(data, priority, state);
    }

    @Override
    public void append(ByteBuffer header, ByteBuffer[] data, byte priority, IoState state) {
        writeBatch.append(header, data, priority, state);
    }

    @Override
    public void appendZeroSizePacket(IoState ioState) {

        writeBatch.appendZeroSizePacket(ioState);
    }

    @Override
    public void cancelNow(IoState state) {
        writeBatch.cancelNow(state);
    }

    @Override
    public void flushAndCancel(IoState state) {
        writeBatch.flushAndCancel(state);
    }

    @Override
    public byte minimumPriority() {
        return writeBatch.minimumPriority();
    }

    @Override
    public byte maximumPriority() {
        return writeBatch.maximumPriority();
    }

    @Override
    public void freeBuffers() {
        writeBatch.freeBuffers();
    }

    @Override
    public synchronized void setSecure(SSLEngine engine) {

        if(writeBatch instanceof UnSecureUnPooledWriteBatch) {

            writeBatch = ((UnSecureUnPooledWriteBatch) writeBatch).toSecureMode(engine);
        }//else fuck it and let it go !
    }

    @Override
    public State checkState() {
        return writeBatch.checkState();
    }

    @Override
    public boolean writeToChannel(SocketChannel socketChannel, IoState state) throws IOException {
        return writeBatch.writeToChannel(socketChannel, state);
    }


    @Override
    public long totalSentBytes() {
        return writeBatch.totalSentBytes();
    }

    @Override
    public long totalSentPackets() {
        return writeBatch.totalSentPackets();
    }
}
