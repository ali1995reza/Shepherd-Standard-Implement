package shepherd.standard.datachannel.standard.iobatch.unpooled.w;

import shepherd.standard.datachannel.standard.iobatch.WriteBatch;
import shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSizeController;
import shepherd.utils.transport.nio.model.IoState;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSize.*;
import static shepherd.standard.datachannel.standard.iobatch.unpooled.w.SizeDetectStatic.*;


final class SecureUnPooledWriteBatch implements WriteBatch {


    private final int WRAP_MINIMUM_SIZE;
    private final static int BATCH_MINIMUM_SIZE = KB.toByte(10);


    private ByteBuffer buffer;
    private ByteBuffer bufferToWrite;
    private boolean closeNow = false;
    private boolean flushAndClose = false;
    private boolean canAppend = true;
    private int remainingBytes = 0;
    private int position = 0;
    private final ByteBuffer zeroSizePacket = ByteBuffer.allocateDirect(4)
            .putInt(0);
    private boolean sendZeroBytePacket = false;

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition waitForFlushCond = lock.newCondition();
    private boolean waitForFlush = false;
    private final BufferSizeController controller;
    private final SSLEngine sslEngine;
    private ByteBuffer wrapBuffer;


    private long totalSentBytes;
    private long totalSentPackets;


    public SecureUnPooledWriteBatch(SSLEngine engine ,
                                    BufferSizeController controller)
    {

        sslEngine = engine;
        WRAP_MINIMUM_SIZE = sslEngine.getSession().getPacketBufferSize();


        this.controller = controller;
        buffer = this.controller.mustAllocate(BATCH_MINIMUM_SIZE ,
                BufferSizeController.Section.WRITE_BATCH);
        bufferToWrite = buffer.duplicate();

        bufferToWrite = buffer.duplicate();



        wrapBuffer = this.controller.mustAllocate(
                WRAP_MINIMUM_SIZE ,
                BufferSizeController.Section.WRAP_AND_WRITE
        );

        wrapBuffer.clear();
        wrapBuffer.limit(0); //remaining = 0
    }


    //each priority saved synchronized from upper side !
    @Override
    public void append(ByteBuffer[] data, byte priority , IoState state) {

        if (canAppend) {

            clearBufferIfPossible();

            if(state.isInIoThread())
            {
                copy_IoThread(data , priority , state);
            }else {
                copy_OtherThread(data , priority , state);
            }

            ++totalSentPackets;
        }

    }

    @Override
    public void append(ByteBuffer header, ByteBuffer[] data, byte priority , IoState state) {


        if (canAppend) {

            clearBufferIfPossible();

            if(state.isInIoThread())
            {
                copy_IoThread(header ,  data , priority , state);
            }else {
                copy_OtherThread(header , data , priority , state);
            }

            ++totalSentPackets;
        }
    }


    private void clearBufferIfPossible()
    {
        if(remainingBytes==0) {

            buffer.clear();
            position = 0;


            offerReAllocateAndFix();

        }
    }

    private boolean offerReAllocateAndFix()
    {
        ByteBuffer newBuffer = controller.offerReallocate(
                buffer.capacity() ,
                BATCH_MINIMUM_SIZE ,
                BufferSizeController.Section.WRITE_BATCH
        );

        if(newBuffer==null)return false;

        buffer = newBuffer;
        buffer.clear();
        bufferToWrite = buffer.duplicate();

        return true;
    }


    private void copy_IoThread(ByteBuffer[] data , byte priority , IoState ioState)
    {
        //as it is inside IO-Thread so do not need to lock

        final int size = detectSize(data)+5;

        if(buffer.remaining()>=size)
        {
            buffer.putInt(size-4);
            copy(priority , data , buffer);
            position+=size;
            remainingBytes+=size;
            ioState.doReadAndWrite();
            return;
        }


        final int realSpace = buffer.capacity()-remainingBytes;

        if(realSpace>=size)
        {
            ByteBuffer newBuffer =
                    controller.offerExtend(buffer.capacity() , size ,
                            BufferSizeController.Section.WRITE_BATCH);

            if(newBuffer==null)
            {

                buffer.flip();
                buffer.position(buffer.limit()-remainingBytes);

                buffer.compact();
                buffer.putInt(size-4);
                copy(priority , data , buffer);
                position = buffer.position();
                remainingBytes = position;
                ioState.doReadAndWrite();
                return;
            }else{

                newBuffer.clear();
                newBuffer.put(buffer);
                newBuffer.putInt(size-4);
                copy(priority , data , newBuffer);


                buffer = newBuffer;
                bufferToWrite = buffer.duplicate();
                position = buffer.position();
                remainingBytes = position;


                ioState.doReadAndWrite();


                return;
            }
        }

        //so this remaining bytes need to write !
        ByteBuffer newBuffer =
                controller.mustExtend(
                        buffer.capacity(),
                        size-realSpace,
                        BufferSizeController.Section.WRITE_BATCH//,reason !
                );

        buffer.flip();
        buffer.position(buffer.limit()-remainingBytes);

        newBuffer.clear();
        newBuffer.put(buffer);
        newBuffer.putInt(size-4);
        copy(priority , data , newBuffer);

        buffer = newBuffer;
        bufferToWrite = buffer.duplicate();
        position = buffer.position();
        remainingBytes = position;


        ioState.doReadAndWrite();
    }

    private void copy_IoThread(ByteBuffer header , ByteBuffer[] data , byte priority , IoState ioState)
    {
        //as it is inside IO-Thread so do not need to lock

        final int size = detectSize(header , data)+5;

        if(buffer.remaining()>=size)
        {
            buffer.putInt(size-4);
            copy(priority , header , data , buffer);
            position+=size;
            remainingBytes+=size;
            ioState.doReadAndWrite();
            return;
        }


        final int realSpace = buffer.capacity()-remainingBytes;

        if(realSpace>=size)
        {
            ByteBuffer newBuffer =
                    controller.offerExtend(buffer.capacity() , size ,
                            BufferSizeController.Section.WRITE_BATCH);

            if(newBuffer==null)
            {

                buffer.flip();
                buffer.position(buffer.limit()-remainingBytes);

                buffer.compact();
                buffer.putInt(size-4);
                copy(priority , header , data , buffer);
                position = buffer.position();
                remainingBytes = position;
                ioState.doReadAndWrite();
                return;
            }else{

                newBuffer.clear();
                newBuffer.put(buffer);
                newBuffer.putInt(size-4);
                copy(priority , header , data , newBuffer);


                buffer = newBuffer;
                bufferToWrite = buffer.duplicate();
                position = buffer.position();
                remainingBytes = position;


                ioState.doReadAndWrite();


                return;
            }
        }

        //so this remaining bytes need to write !
        ByteBuffer newBuffer =
                controller.mustExtend(
                        buffer.capacity(),
                        size-realSpace,
                        BufferSizeController.Section.WRITE_BATCH//,reason !
                );

        buffer.flip();
        buffer.position(buffer.limit()-remainingBytes);

        newBuffer.clear();
        newBuffer.put(buffer);
        newBuffer.putInt(size-4);
        copy(priority , header , data , newBuffer);

        buffer = newBuffer;
        bufferToWrite = buffer.duplicate();
        position = buffer.position();
        remainingBytes = position;


        ioState.doReadAndWrite();
    }


    private void copy_OtherThread(ByteBuffer[] data , byte priority , IoState ioState)
    {
        //need to use lock for checking and handling buffer threading !

        final int size = detectSize(data)+5;

        if(buffer.remaining()>=size) {
            buffer.putInt(size-4);
            copy(priority , data , buffer);
            increaseTotalBufferedBytes(size , ioState);
        }
        else if(size>buffer.capacity()) {
            ByteBuffer newBuffer = controller.mustExtend(
                    buffer.capacity() ,
                    size-buffer.remaining() ,
                    BufferSizeController.Section.WRITE_BATCH
            );

            newBuffer.clear();

            buffer.flip();
            buffer.position(buffer.limit()-remainingBytes);

            newBuffer.put(buffer);
            newBuffer.putInt(size-4);
            copy(priority , data , newBuffer);

            //so lock and fix state !
            lock.lock();
            try {

                buffer = newBuffer;
                bufferToWrite = buffer.duplicate(); //state of this buffer will handled in io-thread
                position = buffer.position();
                //current remaining of bytes its too important
                remainingBytes = remainingBytes + size;

                ioState.doReadAndWrite();

            }finally {
                lock.unlock();
            }


        }
        else {

            ByteBuffer newBuffer = controller.offerExtend(
                    buffer.capacity() ,
                    size ,
                    BufferSizeController.Section.WRITE_BATCH
            );

            if(newBuffer!=null) {

                newBuffer.clear();

                buffer.flip();
                buffer.position(buffer.limit() - remainingBytes);

                newBuffer.put(buffer);
                newBuffer.putInt(size - 4);
                copy(priority, data, newBuffer);

                //so lock and fix state !
                lock.lock();
                try {

                    buffer = newBuffer;
                    bufferToWrite = buffer.duplicate(); //state of this buffer will handled in io-thread
                    position = buffer.position();
                    //current remaining of bytes its too important fot me to handel !
                    remainingBytes = remainingBytes + size;

                    ioState.doReadAndWrite();

                } finally {
                    lock.unlock();
                }
            }else {

                waitAndClearBuffer();
                offerReAllocateAndFix();


                buffer.clear();
                position = 0;
                remainingBytes = 0;
                buffer.putInt(size-4);
                copy(priority , data , buffer);
                increaseTotalBufferedBytes(size , ioState);
            }

        }
    }

    private void copy_OtherThread(ByteBuffer header , ByteBuffer[] data , byte priority , IoState ioState)
    {
        //need to use lock for checking and handling buffer threading !

        final int size = detectSize(header , data)+5;

        if(buffer.remaining()>=size) {
            buffer.putInt(size-4);
            copy(priority , header , data , buffer);
            increaseTotalBufferedBytes(size , ioState);
        }
        else if(size>buffer.capacity()) {
            ByteBuffer newBuffer = controller.mustExtend(
                    buffer.capacity() ,
                    size-buffer.remaining() ,
                    BufferSizeController.Section.WRITE_BATCH
            );

            newBuffer.clear();

            buffer.flip();
            buffer.position(buffer.limit()-remainingBytes);

            newBuffer.put(buffer);
            newBuffer.putInt(size-4);
            copy(priority, header , data , newBuffer);

            //so lock and fix state !
            lock.lock();
            try {

                buffer = newBuffer;
                bufferToWrite = buffer.duplicate(); //state of this buffer will handled in io-thread
                position = buffer.position();
                //current remaining of bytes its too important
                remainingBytes = remainingBytes + size;

                ioState.doReadAndWrite();

            }finally {
                lock.unlock();
            }


        }
        else {

            ByteBuffer newBuffer = controller.offerExtend(
                    buffer.capacity() ,
                    size ,
                    BufferSizeController.Section.WRITE_BATCH
            );

            if(newBuffer!=null) {

                newBuffer.clear();

                buffer.flip();
                buffer.position(buffer.limit() - remainingBytes);

                newBuffer.put(buffer);
                newBuffer.putInt(size - 4);
                copy(priority , header, data, newBuffer);

                //so lock and fix state !
                lock.lock();
                try {

                    buffer = newBuffer;
                    bufferToWrite = buffer.duplicate(); //state of this buffer will handled in io-thread
                    position = buffer.position();
                    //current remaining of bytes its too important fot me to handel !
                    remainingBytes = remainingBytes + size;

                    ioState.doReadAndWrite();

                } finally {
                    lock.unlock();
                }
            }else {

                waitAndClearBuffer();
                offerReAllocateAndFix();


                buffer.clear();
                position = 0;
                remainingBytes = 0;
                buffer.putInt(size-4);
                copy(priority , header , data , buffer);
                increaseTotalBufferedBytes(size , ioState);
            }

        }
    }





    @Override
    public void appendZeroSizePacket(IoState ioState) {

        lock.lock();
        try{
            sendZeroBytePacket = true;
            ioState.doReadAndWrite();
        }finally {
            lock.unlock();
        }
    }

    @Override
    public  void cancelNow(IoState state) {
        lock.lock();
        try {
            if (closeNow) {
                return;
            }
            canAppend = false;
            closeNow = true;
            state.cancel();
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void flushAndCancel(IoState state) {
        lock.lock();

        try {
            if (flushAndClose) {
                return;
            }
            synchronized (this) {
                canAppend = false;
                flushAndClose = true;
                state.doReadAndWrite();
            }
        }finally {
            lock.unlock();
        }
    }

    @Override
    public byte minimumPriority() {
        return 1;
    }

    @Override
    public byte maximumPriority() {
        return 10;
    }

    @Override
    public void freeBuffers() {

        //todo nothing !
    }

    @Override
    public void setSecure(SSLEngine engine) {
        //skip for now !
    }

    @Override
    public State checkState() {
        return State.NOTHING;
    }


    private final static int encrypt(SSLEngine engine , ByteBuffer source , ByteBuffer dest) throws IOException {

        SSLEngineResult result = engine.wrap(source , dest);

        if(result.getStatus() == SSLEngineResult.Status.OK)
        {
            return result.bytesConsumed();
        }else if(result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW)
        {
            return 0;
        }else {
            throw new IOException("bad status - ["+result.getStatus()+"]");
        }

    }

    private int encryptData(IoState state) throws IOException
    {
        boolean zp  = false;


        ByteBuffer captured = null;

        lock.lock();
        try {
            if (closeNow) {
                state.cancel();
                return 0;
            } else if(flushAndClose && remainingBytes==0){
                state.cancel();
                return 0;
            }else if (sendZeroBytePacket) {
                zp = true;
                sendZeroBytePacket = false;
            } else {
                bufferToWrite.limit(position);
                int s = position - remainingBytes;
                bufferToWrite.position(s);
                if(!bufferToWrite.hasRemaining()){
                    state.doRead();
                    return 0;
                }

                captured = bufferToWrite;
            }
        }finally {
            lock.unlock();
        }

        wrapBuffer.clear();
        if(zp)
        {
            zeroSizePacket.clear();
            if(encrypt(sslEngine , zeroSizePacket , wrapBuffer)!=4)
                throw new IOException("can not wrap alive packet");

            wrapBuffer.flip();
            return 4;
        }else {
            int size = encrypt(sslEngine , captured , wrapBuffer);
            if(size==0)
                throw new IOException("can not wrap data packet");

            while (captured.hasRemaining()){
                int s = encrypt(sslEngine , captured , wrapBuffer);

                if(s==0)break;

                size+=s;
            }

            wrapBuffer.flip();

            decreaseTotalBufferedBytes(size);

            return size;
        }
    }


    private final boolean offerReAllocateWrapBuffer()
    {
        ByteBuffer buffer = controller.offerReallocate(wrapBuffer.capacity() ,
                WRAP_MINIMUM_SIZE ,
                BufferSizeController.Section.WRAP_AND_WRITE);

        if(buffer==null)return false;

        wrapBuffer = buffer;
        wrapBuffer.clear().limit(0);

        return true;

    }

    @Override
    public boolean writeToChannel(SocketChannel socketChannel , IoState state) throws IOException {
        if(!wrapBuffer.hasRemaining())
        {
            offerReAllocateWrapBuffer();
            int i = encryptData(state);
            if(i==0)return true;
        }

        int wrote = socketChannel.write(wrapBuffer);
        if(wrote<0)
            throw new IllegalStateException("out put closed");

        totalSentBytes +=wrote;

        return true;
    }

    @Override
    public long totalSentBytes() {
        return totalSentBytes;
    }

    @Override
    public long totalSentPackets() {
        return totalSentPackets;
    }

    private void increaseTotalBufferedBytes(int how , IoState state)
    {
        lock.lock();
        try {
            remainingBytes += how;
            position += how;
            state.doReadAndWrite();
        }finally {
            lock.unlock();
        }
    }


    private void waitAndClearBuffer()
    {
        lock.lock();
        try {
            if(remainingBytes==0)
            {
                return;
            }
            waitForFlush = true;
            waitForFlushCond.await();
            waitForFlush = false;
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            lock.unlock();
        }
    }



    private void decreaseTotalBufferedBytes(int how)
    {
        lock.lock();
        try {
            remainingBytes -= how;
            if (remainingBytes == 0) {
                signalIfWaitingForFlush();
            }
        }finally {
            lock.unlock();
        }
    }


    private final void signalIfWaitingForFlush()
    {
        if(waitForFlush)
        {
            waitForFlushCond.signal();
        }
    }

}
