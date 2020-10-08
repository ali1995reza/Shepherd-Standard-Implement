package shepherd.standard.datachannel.standard.iobatch;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public interface ReadBatch {


    void readFromChannel(SocketChannel channel) throws IOException;
    void readFromBuffer(ByteBuffer buffer) throws IOException;

    ByteBuffer[] getPacket() throws IOException;

    void setSecure(SSLEngine engine);

    long totalReceivedBytes();
    long totalReceivedPackets();
}
