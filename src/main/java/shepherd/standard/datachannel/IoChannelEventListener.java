package shepherd.standard.datachannel;

import java.nio.ByteBuffer;

public interface IoChannelEventListener {

    void onChannelDisconnected(IoChannel ioChannel);
    void onNewChannelConnected(IoChannel ioChannel);
    void onDataReceived(IoChannel ioChannel, ByteBuffer[] message, byte priority);
    void onReadRoundEnd(IoChannel ioChannel);
    void onWriteRoundEnd(IoChannel ioChannel);
}
