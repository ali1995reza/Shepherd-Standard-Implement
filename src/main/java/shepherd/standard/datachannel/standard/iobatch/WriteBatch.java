package shepherd.standard.datachannel.standard.iobatch;

import shepherd.utils.transport.nio.model.IoState;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public interface WriteBatch {

    public enum State{

        CANCEL  , READY_TO_WRITE , NOTHING
    }


    void append(ByteBuffer[] data, byte priority, IoState state);
    void append(ByteBuffer header, ByteBuffer[] data, byte priority, IoState state);
    void appendZeroSizePacket(IoState ioState);


    void cancelNow(IoState state);
    void flushAndCancel(IoState state);



    byte minimumPriority();
    byte maximumPriority();


    void freeBuffers();


    void setSecure(SSLEngine engine);


    State checkState();



    boolean writeToChannel(SocketChannel socketChannel, IoState state) throws IOException;


    long totalSentBytes();
    long totalSentPackets();
}
