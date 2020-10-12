package shepherd.standard.datachannel.standard.iobatch.unpooled.w;

import shepherd.standard.datachannel.standard.iobatch.WriteBatch;
import shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSizeController;
import shepherd.utils.transport.nio.model.IoState;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSize.*;

import static shepherd.standard.datachannel.standard.iobatch.unpooled.w.SizeDetectStatic.*;

final class UnSecureUnPooledWriteBatch implements WriteBatch {



    private final static int MINIMUM_SIZE = KB.toByte(10);





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

    private long totalSentBytes;
    private long totalSentPackets;


    public UnSecureUnPooledWriteBatch(BufferSizeController controller)
    {
        this.controller = controller;
        buffer = this.controller.mustAllocate(MINIMUM_SIZE ,
                BufferSizeController.Section.WRITE_AND_BATCH);
        bufferToWrite = buffer.duplicate();
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
                MINIMUM_SIZE ,
                BufferSizeController.Section.WRITE_AND_BATCH
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
                            BufferSizeController.Section.WRITE_AND_BATCH);

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
                        BufferSizeController.Section.WRITE_AND_BATCH//,reason !
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
                            BufferSizeController.Section.WRITE_AND_BATCH);

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
                        BufferSizeController.Section.WRITE_AND_BATCH//,reason !
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
                    BufferSizeController.Section.WRITE_AND_BATCH
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
                    BufferSizeController.Section.WRITE_AND_BATCH
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
                    BufferSizeController.Section.WRITE_AND_BATCH
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
                    BufferSizeController.Section.WRITE_AND_BATCH
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
    public void cancelNow(IoState state) {
        lock.lock();
        try {
            if (closeNow) {
                return;
            }
            canAppend = false;
            closeNow = true;
            state.cancel();
            signalIfWaitingForFlush();
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


    @Override
    public boolean writeToChannel(SocketChannel socketChannel , IoState state) throws IOException {

        boolean zeroPack = false;

        ByteBuffer captureBuffer = null;

        lock.lock();

        try {
            if (closeNow) {
                state.cancel();
                return true;
            } else if (sendZeroBytePacket) {
                zeroPack = true;
            } else {
                bufferToWrite.clear();
                bufferToWrite.limit(position);
                int s = position - remainingBytes;
                bufferToWrite.position(s);

                captureBuffer = bufferToWrite;
            }
        }finally {
            lock.unlock();
        }

        if(zeroPack)
        {
            if(!zeroSizePacket.hasRemaining())
                zeroSizePacket.clear();

            int wrote = socketChannel.write(zeroSizePacket);

            if(wrote<0)
                throw new IllegalStateException("output closed !");

            if(!zeroSizePacket.hasRemaining())
                sendZeroBytePacket = false;

            totalSentBytes +=wrote;

        }else {
            int wrote = socketChannel.write(captureBuffer);

            if(wrote<0)
                throw new IllegalStateException("output closed !");


            totalSentBytes +=wrote;

            decreaseTotalBufferedBytes(wrote , state);
        }


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

    private void setPosition(int i)
    {
        lock.lock();
        try {
            position = i;
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



    private void decreaseTotalBufferedBytes(int how , IoState state)
    {
        lock.lock();
        try {
            if (closeNow) {
                state.cancel();
                signalIfWaitingForFlush();
            }
            remainingBytes -= how;
            if (remainingBytes == 0) {
                signalIfWaitingForFlush();
                if (flushAndClose) {
                    state.cancel();
                } else if (!sendZeroBytePacket) {
                    state.doRead();
                }
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

    WriteBatch toSecureMode(SSLEngine engine)

    {
        return new SecureUnPooledWriteBatch(engine , controller);
    }
}
