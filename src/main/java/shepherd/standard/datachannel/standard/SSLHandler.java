package shepherd.standard.datachannel.standard;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public final class SSLHandler {


    private SSLEngine sslEngine;
    private ByteBuffer wrapBuffer;
    private boolean wrapBufferCleared = true;
    private ByteBuffer unwrapBuffer;
    private ByteBuffer bufferToRead;
    private SSLEngineResult.HandshakeStatus handshakeStatus;
    private SocketChannel socket;

    private long totalSentBytes;
    private long totalReceivedBytes;

    public SSLHandler(SSLContext context , SocketChannel socketChannel , boolean client) throws IOException
    {
        this(context,socketChannel,client,null);
    }

    public SSLHandler(SSLContext context , SocketChannel socketChannel , boolean client , SSLParameters parameters) throws IOException
    {
        InetSocketAddress socketAddress = (InetSocketAddress)socketChannel.getRemoteAddress();
        if(!socketChannel.isOpen())
        {
            throw new IOException("socket channel not open ");
        }

        socket = socketChannel;


        sslEngine = context.createSSLEngine(socketAddress.getHostName() , socketAddress.getPort());
        sslEngine.setUseClientMode(client);
        if(parameters!=null)
        {
            sslEngine.setSSLParameters(parameters);
        }
        sslEngine.beginHandshake();
        handshakeStatus = sslEngine.getHandshakeStatus();

        unwrapBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        wrapBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        bufferToRead = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());

    }


    @Deprecated
    private SSLHandler(SSLEngine engine) throws SSLException
    {
        if(engine==null)
        {
            throw new NullPointerException();
        }

        if(engine.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)
        {
            throw new SSLException("Illegal SSL handshake state");
        }
        sslEngine = engine;
        sslEngine.beginHandshake();
        handshakeStatus = sslEngine.getHandshakeStatus();

        unwrapBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getPacketBufferSize());
        wrapBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getPacketBufferSize());
        bufferToRead = ByteBuffer.allocateDirect(sslEngine.getSession().getPacketBufferSize());
    }


    private SSLEngineResult.HandshakeStatus setHandshakeStatus(SSLEngineResult.HandshakeStatus s) throws SSLException
    {
        handshakeStatus = s;
        if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING))
        {
            throw new SSLException("Illegal handshake status - NOT_HANDSHAKING");
        }
        return handshakeStatus;
    }

    private boolean compareHandShakeStatus(SSLEngineResult.HandshakeStatus status)
    {
        return handshakeStatus == status;
    }

    private SSLEngineResult.HandshakeStatus doTask()throws SSLException
    {
        if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NEED_TASK))
        {
            Runnable r = sslEngine.getDelegatedTask();
            if(r == null)
            {
                throw new SSLException("protocol violation");
            }

            r.run();
            setHandshakeStatus(sslEngine.getHandshakeStatus());

            if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NEED_TASK))
            {
                return doTask();
            }else
            {

                return handshakeStatus;
            }
        }else
        {
            throw new SSLException("protocol violation");
        }
    }


    private SSLEngineResult.HandshakeStatus doUnwrap()throws IOException
    {
        if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NEED_UNWRAP))
        {
            bufferToRead.flip();
            SSLEngineResult unwrapResult = sslEngine.unwrap(bufferToRead  , unwrapBuffer);
            bufferToRead.compact();
            setHandshakeStatus(unwrapResult.getHandshakeStatus());
            if(unwrapResult.getStatus() == SSLEngineResult.Status.OK)
            {
                //so it means unwrapBuffer done

                unwrapBuffer.clear();
                if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NEED_UNWRAP) && bufferToRead.hasRemaining())
                {
                    return doUnwrap();
                }else if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NEED_TASK))
                {
                    doTask();
                    if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NEED_UNWRAP))
                    {
                        return doUnwrap();
                    }

                    return handshakeStatus;
                }
            }else if(unwrapResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
            {
                throw new SSLException("ssl protocol violation - buffer overflow on unwrapBuffer");
            }else if(unwrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
            {
                throw new IOException("connection closed");
            }

            return handshakeStatus;
        }else
        {
            throw new SSLException("ssl protocol violation");
        }
    }

    public SSLEngineResult.HandshakeStatus unwrap() throws IOException
    {
        int read = socket.read(bufferToRead);
        if(read<0)
        {
            throw new IOException("connection input closed");
        }

        totalReceivedBytes+=read;

        return doUnwrap();
    }


    private SSLEngineResult.HandshakeStatus doWrap() throws IOException
    {
        if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NEED_WRAP))
        {
            if(wrapBufferCleared)
            {
                wrapBuffer.clear();
                SSLEngineResult wrapResult = sslEngine.wrap(ByteBuffer.allocate(0),wrapBuffer);
                if(wrapResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
                {
                    throw new SSLException("buffer overflow occurs in wrap operation");
                }else if(wrapResult.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW)
                {
                    //in a wrap result this must never occurs
                    throw new SSLException("buffer underflow occurs in wrap operation");
                }else if (wrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                {
                    throw new IOException("socket closed during wrap operation");
                }

                wrapBuffer.flip();
                wrapBufferCleared = false;
                setHandshakeStatus(wrapResult.getHandshakeStatus());
                return handshakeStatus;
            }

            if(wrapBuffer.hasRemaining())
            {
                return SSLEngineResult.HandshakeStatus.NEED_WRAP;
            }else
            {
                wrapBuffer.clear();
                wrapBufferCleared = true;
                if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NEED_TASK))
                {
                    doTask();
                    if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NEED_WRAP))
                    {
                        return doWrap();
                    }
                }else if(compareHandShakeStatus(SSLEngineResult.HandshakeStatus.NEED_WRAP))
                {
                    return doWrap();
                }
                return handshakeStatus;
            }
            //so handle it please !
        }else
        {
            throw new SSLException("protocol violation - request to wrap data but status is "+handshakeStatus);
        }
    }

    public SSLEngineResult.HandshakeStatus wrap() throws IOException
    {
        SSLEngineResult.HandshakeStatus status = doWrap();
        int wrote = socket.write(wrapBuffer);
        if(wrote<0)
            throw new IOException("socket output closed");

        totalSentBytes+=wrote;

        return status;
    }

    public long totalReceivedBytes() {
        return totalReceivedBytes;
    }

    public long totalSentBytes() {
        return totalSentBytes;
    }

    public ByteBuffer bufferToRead() {
        return bufferToRead;
    }

    public SSLEngineResult.HandshakeStatus handshakeStatus() {
        return handshakeStatus;
    }


    public SSLEngine sslEngine() {
        return sslEngine;
    }


    private static SSLEngine createSSLEngine(SSLContext context , SocketChannel channel)throws IOException
    {
        if(!channel.isOpen())
        {
            throw new IOException("socket channel not open ");
        }
        InetSocketAddress address = (InetSocketAddress)channel.getRemoteAddress();
        SSLEngine engine = context.createSSLEngine(address.getHostName(),address.getPort());
        return engine;
    }

    public static SSLEngine establishSSLSession(SSLContext context , SocketChannel channel , SSLParameters parameters , boolean clientSide) throws IOException
    {
        SSLEngine engine = createSSLEngine(context, channel);
        if(parameters!=null)
            engine.setSSLParameters(parameters);
        engine.setUseClientMode(clientSide);
        engine.setEnabledCipherSuites(engine.getSupportedCipherSuites());
        ByteBuffer wrap = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        ByteBuffer unwrap = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        ByteBuffer bufferToRead = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        boolean readBytesFlag = false;
        engine.beginHandshake();
        SSLEngineResult.HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
        if(handshakeStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)
        {
            throw new SSLException("can not start the handshake");
        }
        loop : while (handshakeStatus!= SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                && handshakeStatus!= SSLEngineResult.HandshakeStatus.FINISHED)
        {
            if(handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP)
            {
                SSLEngineResult wrapResult = engine.wrap(ByteBuffer.allocateDirect(0),wrap);
                if(wrapResult.getStatus() == SSLEngineResult.Status.OK)
                {
                    wrap.flip();
                    channel.write(wrap);
                    wrap.clear();

                    if(wrapResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED)
                    {
                        handshakeStatus = SSLEngineResult.HandshakeStatus.FINISHED;
                        break;
                    }else if(wrapResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)
                    {
                        throw new SSLException("bad handshake status returned from wrapBuffer operation ");
                    }
                    handshakeStatus = wrapResult.getHandshakeStatus();
                } else if(wrapResult.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW)
                {
                    throw new SSLException("an unknown exception throw - Unknown code : 1");
                }else if(wrapResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
                {
                    throw new SSLException("an unknown exception throw - Unknown code : 2");
                }else if(wrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                {
                    throw new SSLException("engine closed in wrap operation");
                }
            }else if(handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP)
            {
                boolean needRead = false;
                condition : if(readBytesFlag)
                {
                    SSLEngineResult unwrapResult = engine.unwrap(bufferToRead,unwrap);
                    if(unwrapResult.getStatus() == SSLEngineResult.Status.OK)
                    {
                        if(bufferToRead.hasRemaining()) {
                            bufferToRead.compact();
                            bufferToRead.flip();
                            readBytesFlag = true;
                        }else
                        {
                            bufferToRead.clear();
                            readBytesFlag = false;
                        }
                        unwrap.clear();
                        if (unwrapResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED)
                        {
                            handshakeStatus = SSLEngineResult.HandshakeStatus.FINISHED;
                            break loop;
                        }else if(unwrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                        {
                            throw new SSLException("engine closed in wrapBuffer operation");
                        }
                        handshakeStatus = unwrapResult.getHandshakeStatus();
                        continue ;
                    }
                    else if(unwrapResult.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW)
                    {
                        bufferToRead.compact();
                        needRead = true;
                        break condition;
                    }else if(unwrapResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
                    {

                        throw new SSLException("an unknown exception throw - Unknown code : 3");
                    }else if(unwrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                    {
                        throw new SSLException("engine closed with no reason in unwrapBuffer operation");
                    }
                }
                while (!readBytesFlag || needRead)
                {
                    if(channel.read(bufferToRead) < 0)
                    {
                        throw new IOException("channel closed ");
                    }
                    bufferToRead.flip();
                    SSLEngineResult unwrapResult = engine.unwrap(bufferToRead,unwrap);
                    if(unwrapResult.getStatus() == SSLEngineResult.Status.OK)
                    {
                        if(bufferToRead.hasRemaining()) {
                            bufferToRead.compact();
                            bufferToRead.flip();
                            readBytesFlag = true;
                        }else
                        {
                            bufferToRead.clear();
                            readBytesFlag = false;
                        }
                        unwrap.clear();
                        if (unwrapResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED)
                        {
                            handshakeStatus = SSLEngineResult.HandshakeStatus.FINISHED;
                            break loop;
                        }else if(unwrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                        {
                            throw new SSLException("engine closed with no reason in wrapBuffer operation");
                        }
                        handshakeStatus = unwrapResult.getHandshakeStatus();
                        break;
                    }
                    else if(unwrapResult.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW)
                    {
                        bufferToRead.compact();
                        continue;
                    }else if(unwrapResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
                    {

                        throw new SSLException("an unknown exception throw - Unknown code : 3");
                    }else if(unwrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                    {
                        throw new SSLException("engine closed with no reason in unwrapBuffer operation");
                    }
                }
            }else if(handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_TASK)
            {
                Runnable r;
                while ((r = engine.getDelegatedTask())!=null)
                {
                    r.run();
                }
                handshakeStatus = engine.getHandshakeStatus();
            }
        }
        return engine;
    }


    public static SSLEngine establishSSLSessionAndEnsureDontReadOutboundProtocolData(SSLContext context , SocketChannel channel , SSLParameters parameters , boolean clientSide) throws IOException
    {
        SSLEngine engine = createSSLEngine(context, channel);
        if(parameters!=null)
            engine.setSSLParameters(parameters);
        engine.setUseClientMode(clientSide);
        engine.setEnabledCipherSuites(engine.getSupportedCipherSuites());
        //engine.setEnabledCipherSuites(new String[]{"TLS_RSA_WITH_AES_128_CBC_SHA"});
        ByteBuffer wrap = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        ByteBuffer unwrap = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        ByteBuffer bufferToRead = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        boolean readBytesFlag = false;
        engine.beginHandshake();
        SSLEngineResult.HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
        if(handshakeStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)
        {
            throw new SSLException("can not start the handshake");
        }
        loop : while (handshakeStatus!= SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                && handshakeStatus!= SSLEngineResult.HandshakeStatus.FINISHED)
        {
            if(handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP)
            {
                SSLEngineResult wrapResult = engine.wrap(ByteBuffer.allocateDirect(0),wrap);
                if(wrapResult.getStatus() == SSLEngineResult.Status.OK)
                {
                    wrap.flip();
                    channel.write(wrap);
                    wrap.clear();

                    if(wrapResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED)
                    {
                        handshakeStatus = SSLEngineResult.HandshakeStatus.FINISHED;
                        break;
                    }else if(wrapResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)
                    {
                        throw new SSLException("bad handshake status returned from wrapBuffer operation ");
                    }
                    handshakeStatus = wrapResult.getHandshakeStatus();
                } else if(wrapResult.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW)
                {
                    throw new SSLException("an unknown exception throw - Unknown code : 1");
                }else if(wrapResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
                {
                    throw new SSLException("an unknown exception throw - Unknown code : 2");
                }else if(wrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                {
                    throw new SSLException("engine closed in wrap operation");
                }
            }else if(handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP)
            {
                boolean needRead = false;
                condition : if(readBytesFlag)
                {
                    SSLEngineResult unwrapResult = engine.unwrap(bufferToRead,unwrap);
                    if(unwrapResult.getStatus() == SSLEngineResult.Status.OK)
                    {
                        if(bufferToRead.hasRemaining()) {
                            bufferToRead.compact();
                            bufferToRead.flip();
                            readBytesFlag = true;
                        }else
                        {
                            bufferToRead.clear();
                            readBytesFlag = false;
                        }
                        unwrap.clear();
                        if (unwrapResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED)
                        {
                            handshakeStatus = SSLEngineResult.HandshakeStatus.FINISHED;
                            break loop;
                        }else if(unwrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                        {
                            throw new SSLException("engine closed in wrapBuffer operation");
                        }
                        handshakeStatus = unwrapResult.getHandshakeStatus();
                        continue ;
                    }
                    else if(unwrapResult.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW)
                    {
                        bufferToRead.compact();
                        needRead = true;
                        break condition;
                    }else if(unwrapResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
                    {

                        throw new SSLException("an unknown exception throw - Unknown code : 3");
                    }else if(unwrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                    {
                        throw new SSLException("engine closed with no reason in unwrapBuffer operation");
                    }
                }
                while (!readBytesFlag || needRead)
                {
                    bufferToRead.limit(bufferToRead.position()+1);
                    int read = channel.read(bufferToRead);
                    if(read < 0)
                    {
                        throw new IOException("channel closed ");
                    }
                    bufferToRead.flip();
                    SSLEngineResult unwrapResult = engine.unwrap(bufferToRead,unwrap);
                    if(unwrapResult.getStatus() == SSLEngineResult.Status.OK)
                    {
                        if(bufferToRead.hasRemaining()) {
                            bufferToRead.compact();
                            bufferToRead.flip();
                            readBytesFlag = true;
                        }else
                        {
                            bufferToRead.clear();
                            readBytesFlag = false;
                        }
                        unwrap.clear();
                        if (unwrapResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED)
                        {
                            handshakeStatus = SSLEngineResult.HandshakeStatus.FINISHED;
                            break loop;
                        }else if(unwrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                        {
                            throw new SSLException("engine closed with no reason in wrapBuffer operation");
                        }
                        handshakeStatus = unwrapResult.getHandshakeStatus();
                        break;
                    }
                    else if(unwrapResult.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW)
                    {
                        bufferToRead.compact();
                        continue;
                    }else if(unwrapResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
                    {

                        throw new SSLException("an unknown exception throw - Unknown code : 3");
                    }else if(unwrapResult.getStatus() == SSLEngineResult.Status.CLOSED)
                    {
                        throw new SSLException("engine closed with no reason in unwrapBuffer operation");
                    }
                }
            }else if(handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_TASK)
            {
                Runnable r;
                while ((r = engine.getDelegatedTask())!=null)
                {
                    r.run();
                }
                handshakeStatus = engine.getHandshakeStatus();
            }
        }
        return engine;
    }
}
