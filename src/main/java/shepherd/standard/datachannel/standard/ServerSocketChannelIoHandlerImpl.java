package shepherd.standard.datachannel.standard;

import shepherd.api.logger.LoggerFactory;
import shepherd.api.logger.Logger;
import shepherd.utils.transport.nio.model.IoContext;
import shepherd.utils.transport.nio.model.IoHandler;
import shepherd.utils.transport.nio.model.IoOperation;
import shepherd.utils.transport.nio.model.IoState;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;


public class ServerSocketChannelIoHandlerImpl implements IoHandler {

    private ServerSocketChannel serverSocketChannel;
    private final IoHandlerFactory ioHandlerFactory;


    private final Logger logger;



    public ServerSocketChannelIoHandlerImpl(ServerSocketChannel server , IoHandlerFactory factory)
    {

        serverSocketChannel = server;
        ioHandlerFactory = factory;
        logger = LoggerFactory.factory().getLogger(this);
    }

    @Override
    public void read(IoContext context) throws IOException {
        logger.error("this io handler dose not support reading, " +
                "but processor request for read event - IoHandler : "+this);
    }

    @Override
    public void write(IoContext context) throws IOException {
        logger.error("this io handler dose not support writing, " +
                "but processor request for write event - IoHandler : "+this);
    }

    @Override
    public void accept(IoContext context) throws IOException {
        logger.information("accept new accept request - IoHandler"+this);
        SocketChannel channel = serverSocketChannel.accept();
        logger.information("socket accepted successfully - IoHandler"+this+" , accepted socket :"+channel);
        callAccept(context , channel);
    }

    @Override
    public void connect(IoContext context) throws IOException {
        logger.error("this io handler dose not support connect , but process request for connect event");
    }

    @Override
    public void onException(Throwable e, IoOperation op) {
        logger.error("some error occurs on server io handler -" +
                "IoHandler : "+this+
                " , error.message : "+e.getMessage());
    }


    private void callAccept(IoContext context , SocketChannel channel)
    {
        try{
            SSLContext sslContext = context.getAttribute(Attributes.SSL_CONTEXT);

            IoHandler newIoHandler = null;

            if(sslContext==null)
            {
                //so it means not secure
                newIoHandler = ioHandlerFactory.createUnSecureServerSideIoHandler(channel);
            }else
            {


                newIoHandler =
                        ioHandlerFactory.createSecureServerSideIoHandler(channel , sslContext);
            }
            try{
                logger.information("trying to register accepted socket to context IoProcessor - " +
                        "IoHandler : "+this);
                context.processor().registerIoHandler(newIoHandler);
                logger.information("accepted socket registered to context IoProcessor successfully - " +
                        "IoHandler : "+this);
            }catch (Throwable e)
            {
                logger.error("fail to register accepted socket in context IoProcessor -" +
                        "IoHandler : "+this+
                        " , error.message : "+e.getMessage());


                logger.information("closing accepted socket - IoHandler : "+this);
                try {
                    channel.close();
                }catch (IOException ioEx){}

                logger.information("accepted socket closed - IoHandler : "+this);
                return;
            }


            //context.eventHandler().onNewIoHandlerOpened(newIoHandler);

        }catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public SelectableChannel channel() {

        try {
            serverSocketChannel.configureBlocking(false);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return serverSocketChannel;
    }

    @Override
    public void onIdle(IoContext context , IoOperation operation) {

    }

    @Override
    public void onRegister(IoContext context , IoState state) {
        logger.information("io handler registered to processor - IoHandler : "+this);
        state.doAccept();
    }

    @Override
    public void onCanceled(IoContext context) {
        logger.information("io handler canceled - IoHandler : "+this);
    }



    private String thisAsString;

    @Override
    public String toString() {
        if(this.thisAsString == null)
        {
            try {
                thisAsString = "[ServerSocketChannelIoHandlerImpl - address : "+serverSocketChannel.getLocalAddress()+" ]";
            } catch (Throwable e) {
            }
        }
        return thisAsString;
    }

}
