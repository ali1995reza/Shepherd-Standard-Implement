package shepherd.standard.datachannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public interface IoChannel {

    void send(ByteBuffer header, ByteBuffer[] dataToSend, byte priority) throws IOException;
    void send(ByteBuffer[] dataToSend, byte priority) throws IOException;

    <T> T attach(Object attachment);
    SocketAddress remoteAddress();
    SocketAddress localAddress();
    <T> T attachment();

    void flushAndClose();
    void closeNow();

    long totalSentBytes();
    long totalReceivedBytes();

    long totalSentPackets();
    long totalReceivedPackets();

}
