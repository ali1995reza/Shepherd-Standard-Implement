package shepherd.standard.datachannel.standard;

import shepherd.standard.datachannel.IoChannelEventListener;
import shepherd.standard.datachannel.standard.iobatch.ReadBatch;
import shepherd.standard.datachannel.standard.iobatch.WriteBatch;
import shepherd.api.logger.LoggerFactory;
import shepherd.api.logger.Logger;
import shepherd.utils.transport.nio.model.IoContext;
import shepherd.utils.transport.nio.model.IoHandler;
import shepherd.utils.transport.nio.model.IoOperation;
import shepherd.utils.transport.nio.model.IoState;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;

abstract class AbstractSocketChannelIoHandler implements IoHandler , IoChannelProvider {


    private final static class ImmutableIoState implements IoState{

        private IoState ioState;

        public synchronized void setIoState(IoState ioState) {
            if(this.ioState!=null)
                throw new IllegalStateException("io state can not changed!");
            this.ioState = ioState;
        }

        @Override
        public void doRead() {
            ioState.doRead();
        }

        @Override
        public void doWrite() {
            ioState.doWrite();
        }

        @Override
        public void doAccept() {
            ioState.doAccept();
        }

        @Override
        public void doConnect() {
            ioState.doConnect();
        }

        @Override
        public void doReadAndWrite() {
            ioState.doReadAndWrite();
        }

        @Override
        public void doOperations(IoOperation... ops) {
            ioState.doOperations(ops);
        }

        @Override
        public void doOperation(IoOperation op) {
            ioState.doOperation(op);
        }

        @Override
        public void doNothing() {
            ioState.doNothing();
        }

        @Override
        public void cancel() {
            ioState.cancel();
        }

        @Override
        public boolean isInIoThread() {
            return ioState.isInIoThread();
        }

        @Override
        public void setOperationsToCheckIdle(IoOperation... operations) {
            ioState.setOperationsToCheckIdle(operations);
        }

        @Override
        public boolean containsOperation(IoOperation ioOperation) {
            return ioState.containsOperation(ioOperation);
        }

        @Override
        public boolean containsOperations(IoOperation... ioOperations) {
            return ioState.containsOperations(ioOperations);
        }

        @Override
        public boolean isCanceled() {
            return ioState.isCanceled();
        }

        @Override
        public IoHandler ioHandler() {
            return ioState.ioHandler();
        }
    }


    //-------------------------------------fields-----------------------------------

    protected final IoState ioState;
    protected ReadBatch readBatch;
    protected WriteBatch writeBatch;
    private SSLHandler sslHandler;
    private SSLEngine sslEngine;
    private final boolean secure;
    private boolean handShaking = false;
    protected final SocketChannel socketChannel;
    private final static byte DEFAULT_ACTIVE_COUNT = 2;
    private byte active = DEFAULT_ACTIVE_COUNT;
    protected final Logger logger;
    protected final IoChannelEventListener eventListener;

    protected long totalSSLProtocolSentBytes = 0l;
    protected long totalSSLProtocolReceivedBytes = 0l;




    //----------------------------------constructors----------------------------------



    protected AbstractSocketChannelIoHandler(SocketChannel channel, SSLEngine engine, IoChannelEventListener eventListener)
    {
        this.eventListener = eventListener;
        ioState = new ImmutableIoState();
        this.socketChannel = channel;

        logger = LoggerFactory.factory().getLogger(this);

        if(engine!=null)
        {
            this.sslEngine = engine;
            secure = true;
            logger.information("Construct new secure socket channel io handler");
        }else {

            secure = false;
            logger.information("Construct new unSecure socket channel io handler");
        }


    }


    protected AbstractSocketChannelIoHandler(SocketChannel channel, SSLHandler handler, IoChannelEventListener eventListener)
    {
        this.eventListener = eventListener;
        ioState = new ImmutableIoState();

        this.socketChannel = channel;

        logger = LoggerFactory.factory().getLogger(this);

        if(handler!=null)
        {
            this.sslHandler = handler;
            handShaking = true;
            secure = true;
            logger.information("Construct new secure socket channel io handler - [socket : {}] - server-side" , channel);
        }else {

            secure = false;
            logger.information("Construct new unSecure socket channel io handler - [socket : {}] - server-side" , channel);
        }


    }


    protected AbstractSocketChannelIoHandler(SocketChannel channel, IoChannelEventListener eventListener)
    {
        this(channel,(SSLEngine) null, eventListener);
    }




    //------------------------------abstract-methods--------------------------------



    public abstract WriteBatch writeBatch(IoContext ioContext);
    public abstract ReadBatch readBatch(IoContext context);
    public abstract void whenRegistered(IoContext context , IoState state);
    public abstract void afterHandShakeDone(IoContext context , IoState state);
    public abstract void handleIdle(IoOperation operation) throws IOException;
    public abstract void afterCanceled(IoContext context);
    public abstract void onDataReceived(IoContext context , ByteBuffer[] data);
    public abstract void onReadRoundEnd(IoContext context);
    public abstract void onWriteRoundEnd(IoContext context);




    //----------------------------public-methods---------------------------------------


    public void write(ByteBuffer[] data , byte priority)
    {

        if(ioState.isCanceled()) {
            logger.debug("trying to write data <write(ByteBuffer[] data , byte priority)> but handler already closed");
            return;
        }

        writeBatch.append(data , priority , ioState);
    }



    public void write(ByteBuffer header , ByteBuffer[] data  , byte priority)
    {

        if(ioState.isCanceled()) {
            logger.debug("trying to write data <write(ByteBuffer header , ByteBuffer[] data , byte priority)> but handler already closed");
            return;
        }

        writeBatch.append(header , data ,priority , ioState);

    }





    public final SocketAddress remoteAddress() {

        try {
            return socketChannel.getRemoteAddress();
        } catch (IOException e) {
            return null;
        }
    }

    public final SocketAddress localAddress(){

        try {
            return socketChannel.getLocalAddress();
        } catch (IOException e) {
            return null;
        }
    }




    @Override
    public final void onRegister(IoContext context, IoState state) {

        ((ImmutableIoState)ioState).setIoState(state);


        readBatch = readBatch(context);

        if(ioState.isCanceled())
            return;

        if(handShaking)
        {
            //so getSTATUS TO WRITE !
            if(sslHandler.handshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP)
            {
                ioState.doWrite();
            }else if(sslHandler.handshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP)
            {
                ioState.doRead();
            }else
            {
                throw new IllegalStateException("SSL state not valid for start - state : "+ sslHandler.handshakeStatus());
            }

        }else{

            if(secure)
                readBatch.setSecure(sslEngine);

            ioState.doReadAndWrite();
        }


        initWriteBatch(context);

        if(ioState.isCanceled())
            return;



        ioState.setOperationsToCheckIdle(IoOperation.READ , IoOperation.WRITE);


        whenRegistered(context , state);

    }

    protected final boolean isHandshakeDone()
    {
        return !handShaking;
    }




    private final void handleCancel()
    {
        writeBatch.freeBuffers();
    }

    @Override
    public final void onCanceled(IoContext context) {

        handleCancel();

        afterCanceled(context);

    }


    @Override
    public final void read(IoContext context) throws IOException {

        setActive();

        if(handShaking)
        {
            //so handle it please


            handleSSLEvent(context , IoOperation.READ);
            return;
        }





        readBatch.readFromChannel(socketChannel);


        ByteBuffer[] packet;
        while (!ioState.isCanceled() && (packet = readBatch.getPacket())!=null)
        {
            onDataReceived(context , packet);
        }

        if(ioState.isCanceled())
        {
            return;
        }

        onReadRoundEnd(context);
    }

    @Override
    public final void write(IoContext context) throws IOException {

        if(handShaking)
        {
            //so handle it please
            handleSSLEvent(context , IoOperation.WRITE);
            return;
        }

        doWriting();

        if(ioState.isCanceled())
            return;

        onWriteRoundEnd(context);
    }

    @Override
    public void accept(IoContext context) throws IOException {

    }

    @Override
    public void connect(IoContext context) throws IOException {

    }




    @Override
    public final void onIdle(IoContext context , IoOperation operation) throws IOException {



        if(operation==IoOperation.READ)
        {
            if(!isActive())
            {
                logger.information("connection is idle : {}" , socketChannel);
                throw new IOException("connection idle");
            }else
            {
                handleIdle(operation);
            }
        }else if(operation == IoOperation.WRITE)
        {
            if(!handShaking) {

                writeBatch.appendZeroSizePacket(ioState);

                ioState.doReadAndWrite();
            }
            handleIdle(operation);
        }else {
            handleIdle(operation);
        }

    }






    @Override
    public final SelectableChannel channel() {
        try {
            socketChannel.configureBlocking(false);
        } catch (IOException e) {
            logger.exception(e);
            throw new IllegalStateException(e);
        }
        return socketChannel;
    }



    private final boolean isActive()
    {
        return --active>=0;
    }

    private final void setActive()
    {

        if(active==DEFAULT_ACTIVE_COUNT)return;


        active = DEFAULT_ACTIVE_COUNT;
    }

    protected void initWriteBatch(IoContext ioContext)
    {
        writeBatch = writeBatch(ioContext);

        if(writeBatch ==null) throw new NullPointerException("provided write queue is null");



        if(!secure) return;
        if(handShaking) return;


        writeBatch.setSecure(sslEngine);
    }


    private final void handleSSLEvent(IoContext context , IoOperation operation) throws IOException {
        if(operation == IoOperation.READ)
        {

            SSLEngineResult.HandshakeStatus status = sslHandler.unwrap();
            handleHandSHakeStatus(context , status);
        }else if(operation == IoOperation.WRITE)
        {
            SSLEngineResult.HandshakeStatus status = sslHandler.wrap();
            handleHandSHakeStatus(context , status);
        }
    }

    private final void handleHandSHakeStatus(IoContext context , SSLEngineResult.HandshakeStatus status)
    {
        if(status == SSLEngineResult.HandshakeStatus.NEED_UNWRAP)
        {
            ioState.doRead();
        }else if(status == SSLEngineResult.HandshakeStatus.NEED_WRAP)
        {
            ioState.doWrite();
        }else if(status == SSLEngineResult.HandshakeStatus.FINISHED)
        {
            ioState.doRead();
            whenHandShakeDone(context);
        }
    }

    private final void whenHandShakeDone(IoContext ioContext)
    {
        handShaking = false;
        sslEngine = sslHandler.sslEngine();
        //todo read dat shit data please !!!!

        totalSSLProtocolReceivedBytes = sslHandler.totalReceivedBytes();
        totalSSLProtocolSentBytes = sslHandler.totalSentBytes();

        sslHandler = null;
        readBatch.setSecure(sslEngine);
        writeBatch.setSecure(sslEngine);


        afterHandShakeDone(ioContext , ioState);
    }


    protected void doWriting() throws IOException {
        writeBatch.writeToChannel(socketChannel , ioState);
    }


    protected final boolean isSecure()
    {
        return secure;
    }



}
