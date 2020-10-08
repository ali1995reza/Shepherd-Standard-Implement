package shepherd.standard.datachannel.standard;

import shepherd.api.config.ConfigurationKey;

import javax.net.ssl.SSLContext;

public class StandardDataChannelConfigurations {


    public final static ConfigurationKey<Integer> NUMBER_OF_IO_THREADS =
            new ConfigurationKey<>("number.of.io.threads");
    public final static ConfigurationKey<SSLContext> SSL_CONTEXT =
            new ConfigurationKey<>("ssl.context");


    public final static String SOCKET_RECEIVE_BUFFER_SIZE = "socket.receive.buffer.size";
    public final static String SOCKET_SEND_BUFFER_SIZE = "socket.send.buffer.size";


}
