package shepherd.standard.datachannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collection;

public interface IoChannelCenter {


    Collection<IoChannel> connectedChannels();
    void setDataChannelEventListener(IoChannelEventListener eventListener);
    IoChannel connect(SocketAddress address) throws IOException;
    IoChannel connect(SocketAddress address , Object attachment) throws IOException;
    IoChannel findChannelByAddress(SocketAddress address);

    void start(SocketAddress address) throws IOException;
    void stop();

}
