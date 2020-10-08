package shepherd.standard.datachannel.standard;

import shepherd.standard.datachannel.IoChannel;
import shepherd.standard.datachannel.IoChannelEventListener;
import shepherd.standard.datachannel.standard.iobatch.BatchFactory;
import shepherd.standard.datachannel.standard.iobatch.ReadBatch;
import shepherd.standard.datachannel.standard.iobatch.WriteBatch;
import shepherd.utils.buffer.Utils;
import shepherd.utils.transport.nio.model.IoContext;
import shepherd.utils.transport.nio.model.IoHandler;
import shepherd.utils.transport.nio.model.IoOperation;
import shepherd.utils.transport.nio.model.IoState;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

public class SocketChannelIoHandlerImpl extends AbstractSocketChannelIoHandler implements IoChannel {




    public static class Factory implements IoHandlerFactory{


        private final IoChannelEventListener eventListener;

        public Factory(IoChannelEventListener eventListener) {
            this.eventListener = eventListener;
        }

        @Override
        public void initialize(IoContext context) {

        }

        @Override
        public IoHandler createUnSecureClientSideIoHandler(SocketChannel channel) throws IOException {
            channel.setOption(StandardSocketOptions.SO_RCVBUF , 10*1024*1024);
            channel.setOption(StandardSocketOptions.SO_RCVBUF , 10*1024*1024);
            SocketChannelIoHandlerImpl handler =
                    new SocketChannelIoHandlerImpl(channel , true  , eventListener);

            return handler;
        }

        @Override
        public IoHandler createUnSecureServerSideIoHandler(SocketChannel channel) throws IOException {
            channel.setOption(StandardSocketOptions.SO_RCVBUF , 10*1024*1024);
            channel.setOption(StandardSocketOptions.SO_RCVBUF , 10*1024*1024);
            SocketChannelIoHandlerImpl handler =
                    new SocketChannelIoHandlerImpl(channel , false  , eventListener);

            return handler;
        }

        @Override
        public IoHandler createSecureServerSideIoHandler(SocketChannel channel, SSLContext sslContext) throws IOException {
            channel.setOption(StandardSocketOptions.SO_RCVBUF , 10*1024*1024);
            channel.setOption(StandardSocketOptions.SO_RCVBUF , 10*1024*1024);
            SocketChannelIoHandlerImpl handler =
                    new SocketChannelIoHandlerImpl(channel , new SSLHandler(
                            sslContext ,
                            channel ,
                            false
                    ) , false  , eventListener);
            return handler;
        }

        @Override
        public IoHandler createSecureClientSideIoHandler(SocketChannel channel, SSLContext sslContext) throws IOException {
            channel.setOption(StandardSocketOptions.SO_RCVBUF , 10*1024*1024);
            channel.setOption(StandardSocketOptions.SO_RCVBUF , 10*1024*1024);
            SSLEngine engine = SSLHandler.establishSSLSessionAndEnsureDontReadOutboundProtocolData(sslContext ,
                    channel , null , true);
            SocketChannelIoHandlerImpl handler =
                    new SocketChannelIoHandlerImpl(channel , engine , true  , eventListener);
            return handler;
        }

    }



    private final boolean clientSide;
    private Object attachment;
    private final CountDownLatch closeLatch = new CountDownLatch(1);




    private SocketChannelIoHandlerImpl(SocketChannel channel, SSLEngine engine  , boolean cs , IoChannelEventListener eventListener) {
        super(channel, engine, eventListener);

        clientSide = cs;
    }

    private SocketChannelIoHandlerImpl(SocketChannel channel, SSLHandler handler , boolean cs , IoChannelEventListener eventListener) {
        super(channel, handler, eventListener);

        clientSide = cs;
    }

    private SocketChannelIoHandlerImpl(SocketChannel channel, boolean cs , IoChannelEventListener eventListener) {
        super(channel, eventListener);

        clientSide = cs;
    }


    @Override
    public void send(ByteBuffer header, ByteBuffer[] dataToSend, byte priority) throws IOException {
        write(header, dataToSend, priority);
    }

    @Override
    public void send(ByteBuffer[] dataToSend, byte priority) throws IOException {
        write(dataToSend, priority);
    }

    @Override
    public <T> T attach(Object attachment) {
        Object old = this.attachment;
        this.attachment = attachment;
        return (T)old;
    }

    @Override
    public <T> T attachment() {
        return (T)attachment;
    }

    @Override
    public void flushAndClose() {
        writeBatch.flushAndCancel(ioState);
    }

    @Override
    public void closeNow() {
        writeBatch.cancelNow(ioState);
    }



    @Override
    public WriteBatch writeBatch(IoContext context) {
        BatchFactory batchFactory =
                context.getAttribute(Attributes.BATCH_FACTORY);

        return batchFactory.createWriteBatch();
    }

    @Override
    public ReadBatch readBatch(IoContext context) {
        BatchFactory batchFactory =
                context.getAttribute(Attributes.BATCH_FACTORY);

        return batchFactory.createReadBatch();
    }

    @Override
    public void whenRegistered(IoContext context, IoState state) {
        if(!clientSide && isHandshakeDone())
        {
            eventListener.onNewChannelConnected(this);
        }
    }

    @Override
    public void afterHandShakeDone(IoContext context , IoState state) {
        //so we need to tell the system ioChannel opened !
        if(!clientSide)
        {
            eventListener.onNewChannelConnected(this);
        }
    }

    @Override
    public void afterCanceled(IoContext context) {
        if(closeLatch.getCount()>0) {
            closeLatch.countDown();
            eventListener.onChannelDisconnected(this);
        }
    }

    @Override
    public void onDataReceived(IoContext context, ByteBuffer[] data) {
        eventListener.onDataReceived(this , data , Utils.getByte(data));
    }

    @Override
    public void onReadRoundEnd(IoContext context) {
        eventListener.onReadRoundEnd(this);
    }

    @Override
    public void onWriteRoundEnd(IoContext context) {

    }


    @Override
    public void handleIdle(IoOperation operation) throws IOException {

        //dont need to handle

    }

    @Override
    public IoChannel provideIoChannel() {
        return this;
    }

    @Override
    public void onException(Throwable e, IoOperation op)
    {
        logger.exception("An exception occurs in "+op+" operation" , e);
        closeNow();
    }


    @Override
    public long totalReceivedBytes() {
        return readBatch.totalReceivedBytes()+totalSSLProtocolReceivedBytes;
    }

    @Override
    public long totalSentPackets() {
        return writeBatch.totalSentPackets();
    }

    @Override
    public long totalReceivedPackets() {
        return readBatch.totalReceivedPackets();
    }

    @Override
    public long totalSentBytes() {
        return writeBatch.totalSentBytes()+totalSSLProtocolSentBytes;
    }
}
