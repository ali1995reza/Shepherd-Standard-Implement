package shepherd.standard.datachannel.standard;

import shepherd.utils.transport.nio.model.IoContext;
import shepherd.utils.transport.nio.model.IoHandler;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.channels.SocketChannel;

interface IoHandlerFactory {

    void initialize(IoContext context);
    IoHandler createUnSecureClientSideIoHandler(SocketChannel channel) throws IOException;
    IoHandler createUnSecureServerSideIoHandler(SocketChannel channel) throws IOException;
    IoHandler createSecureServerSideIoHandler(SocketChannel channel, SSLContext sslContext) throws IOException;
    IoHandler createSecureClientSideIoHandler(SocketChannel channel, SSLContext sslContext) throws IOException;

}
