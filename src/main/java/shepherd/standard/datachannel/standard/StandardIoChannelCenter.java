package shepherd.standard.datachannel.standard;

import shepherd.standard.config.ConfigChangeResult;
import shepherd.standard.datachannel.IoChannel;
import shepherd.standard.datachannel.IoChannelCenter;
import shepherd.standard.datachannel.IoChannelEventListener;
import shepherd.standard.datachannel.standard.iobatch.BatchFactory;
import shepherd.standard.datachannel.standard.iobatch.unpooled.UnPooledBatchFactory;
import shepherd.api.config.ConfigurationChangeResult;
import shepherd.api.config.ConfigurationKey;
import shepherd.api.config.IConfiguration;
import shepherd.api.logger.Logger;
import shepherd.api.logger.LoggerFactory;
import shepherd.utils.transport.nio.implement.NIOProcessor;
import shepherd.utils.transport.nio.model.IoHandler;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public final class StandardIoChannelCenter implements IoChannelCenter {


    private NIOProcessor processor;
    private IConfiguration configuration;
    private final Object _sync = new Object();
    private boolean started = false;
    private boolean stopped = false;
    private final IoChannelEventListenerReferenceHolder eventListener;
    private final IoHandlerFactory factory;
    private final Logger logger;
    private final Set<IoChannel> connectedChannels;
    private BatchFactory batchFactory;


    public StandardIoChannelCenter(IConfiguration configuration , IoChannelEventListener eventListener)
    {
        this.configuration = configuration.createSubConfiguration("StandardIoConfig");
        defineConfigurations();
        logger = LoggerFactory.factory().getLogger(this);
        connectedChannels = new HashSet<>();
        this.eventListener = new IoChannelEventListenerReferenceHolder(eventListener);

        factory = new SocketChannelIoHandlerImpl.Factory(this.eventListener);

    }


    @Override
    public Collection<IoChannel> connectedChannels() {
        return connectedChannels;
    }

    private static void checkNotNull(Object o , String msg)
    {
        if(o == null)
            throw new NullPointerException(msg);
    }

    @Override
    public void setDataChannelEventListener(IoChannelEventListener eventListener) {
        synchronized (_sync) {
            this.eventListener.setReference(eventListener);
        }
    }

    @Override
    public IoChannel connect(SocketAddress address) throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.connect(address);
        SSLContext sslContext = configuration.get(StandardDataChannelConfigurations.SSL_CONTEXT);
        IoHandler ioHandler = null;
        if(sslContext!=null)
        {

            ioHandler = factory.createSecureClientSideIoHandler(socketChannel , sslContext);
        }else {
            ioHandler = factory.createUnSecureClientSideIoHandler(socketChannel);
        }
        try {
            processor.registerIoHandler(ioHandler);
        }catch (IOException e)
        {
            try {
                socketChannel.close();
            }catch (IOException ex){}

            throw e;
        }

        IoChannel ioChannel = ((IoChannelProvider)ioHandler).provideIoChannel();
        connectedChannels.add(ioChannel);
        return ioChannel;
    }

    @Override
    public IoChannel findChannelByAddress(SocketAddress address) {
        return null;
    }

    @Override
    public void start(SocketAddress address) throws IOException {
        synchronized (_sync) {
            assertIfStopped("data channel started already");
            assertIfStarted("data channel stopped already");
            assertIfEventListenerIsNull("event listener is null");

            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(address);

            initByConfigs();

            processor.setIdleCheckTime(1000);
            processor.ioContext().setAttribute("1" , new AtomicInteger(0));

            processor.start();


            try {
                processor.registerIoHandler(new ServerSocketChannelIoHandlerImpl(serverSocketChannel , factory));
            } catch (IOException e) {
                //todo handleBy io exception
                processor.stop();
                processor = null;
                throw e;
            }
            started = true;
        }
    }


    private void defineConfigurations()
    {


        configuration.defineConfiguration(
                StandardDataChannelConfigurations.SSL_CONTEXT ,
                null ,
                this::approveConfigChange
        );

        configuration.defineConfiguration(
                StandardDataChannelConfigurations.NUMBER_OF_IO_THREADS ,
                1 ,
                this::approveConfigChange
        );


        batchFactory = new UnPooledBatchFactory();
        batchFactory.initialize(configuration);



    }

    private void initByConfigs()
    {
        int numberOfIoThreads = configuration.
                get(StandardDataChannelConfigurations.NUMBER_OF_IO_THREADS);

        processor = new NIOProcessor(numberOfIoThreads);

        processor.ioContext().setAttribute(
                Attributes.BATCH_FACTORY ,
                batchFactory
        );

        SSLContext context = configuration.
                get(StandardDataChannelConfigurations.SSL_CONTEXT);

        if(context!=null) {
            processor.ioContext().setAttribute(
                    Attributes.SSL_CONTEXT,
                    context
            );
        }

    }

    private ConfigurationChangeResult approveConfigChange(IConfiguration parent , ConfigurationKey confName , Object current , Object val)
    {
        synchronized (_sync) {
            if(confName.equals(StandardDataChannelConfigurations.NUMBER_OF_IO_THREADS))
            {
                if(val instanceof Integer)
                {
                    int i = (Integer)val;

                    if(i<1)
                        return ConfigChangeResult.newFailResult("Number of io threads can not less tha 1");

                    if(started)
                        return ConfigChangeResult.newFailResult("Can not change number of io threads while data channel started");

                    return ConfigChangeResult.newSuccessResult(val);
                }
            }else if(confName.equals(StandardDataChannelConfigurations.SSL_CONTEXT))
            {
                if(started)
                    return ConfigChangeResult.newFailResult("Can not change ssl context");

                if(val instanceof SSLContext)
                {
                    return ConfigChangeResult.newSuccessResult(val);
                }
            }
            return ConfigChangeResult.newFailResult("Can not change config");
        }
    }


    private final void assertIfStarted(String msg)
    {
        if(started)
            throw new IllegalStateException(msg);
    }

    private final void assertIfStopped(String msg)
    {
        if(stopped)
            throw new IllegalStateException(msg);
    }

    private final void assertIfNotStartYet(String msg)
    {
        if(!started)
            throw new IllegalStateException(msg);
    }

    private final void assertIfEventListenerIsNull(String msg)
    {
        if(eventListener==null)
            throw new IllegalStateException(msg);
    }
    @Override
    public void stop() {
        synchronized (_sync)
        {
            assertIfStopped("data channel stopped already");
            assertIfNotStartYet("data channel did not start yet");
            stopped = true;

            //so handle stop please !

            List<IoHandler> handlers = processor.stop();

            for(IoHandler handler:handlers)
            {
                try {
                    handler.channel().close();
                } catch (IOException e) {
                    //todo save logs !
                }
            }

        }
    }
}
