package shepherd.standard.datachannel.standard.iobatch.unpooled.r;

import shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSizeController;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

final class PacketParser {


    private ByteBuffer buffer;
    private final Deque<ByteBuffer> holder;
    private int remainingSizeOfPacket = -1;
    private int bufferedSize = 0;
    private int actualReadPosition = 0;



    private final BufferSizeController sizeController;
    private final int MINIMUM_SIZE;
    private final BufferSizeController.Section section;


    public PacketParser(BufferSizeController sizeController, int minSize, BufferSizeController.Section section)
    {
        this.sizeController = sizeController;
        MINIMUM_SIZE = minSize;
        this.section = section;
        holder = new ArrayDeque<>(100);
        buffer = allocate();
    }

    private final ByteBuffer allocate()
    {
        return sizeController.mustAllocate(
                MINIMUM_SIZE ,
                section
        );
    }

    public final ByteBuffer buffer(){

        return buffer;
    }

    //just call by secure mode !
    public final ByteBuffer newBuffer()
    {
        int h = buffer.position()-actualReadPosition;
        if(h==0)
        {
            allocateNewBuffer();
        }else if(remainingSizeOfPacket<0) {
            allocateNewBufferAndKeepRemainingBytes();
        }else {
            pushCurrentBufferToHolder();
            allocateNewBuffer();
        }
        return buffer;
    }



    public final void accumulateBufferedSize(int read)
    {
        bufferedSize+=read;
    }


    public ByteBuffer[] getPacket() throws IOException{
        return chunkBuffer();
    }


    private ByteBuffer[] chunkBuffer() throws IOException {
        if(remainingSizeOfPacket==-1)
        {
            //so we must detect size of packet !
            if(detectSize())
                return chunkBuffer();

            return null;

        }else
        {

            if(bufferedSize>=remainingSizeOfPacket)
            {
                //ok so handleBy it please !
                ByteBuffer dup = buffer.duplicate().asReadOnlyBuffer();

                dup.position(actualReadPosition);
                dup.limit(actualReadPosition+remainingSizeOfPacket);

                bufferedSize -= remainingSizeOfPacket;
                actualReadPosition += remainingSizeOfPacket;


                remainingSizeOfPacket = -1;


                ByteBuffer[] packet = new ByteBuffer[holder.size()+1];
                int count = 0;
                while (!holder.isEmpty())
                {
                    packet[count++] = holder.pollFirst();
                }
                packet[count] = dup;

                if(buffer.capacity()-actualReadPosition<4)
                {
                    //so no space even for detect size of packet !
                    if(buffer.position()>actualReadPosition)
                    {
                        allocateNewBufferAndKeepRemainingBytes();
                    }else
                    {
                        allocateNewBuffer();
                    }
                }

                return packet;
            }else if(!buffer.hasRemaining())
            {
                pushCurrentBufferToHolder();
                allocateNewBuffer();
            }
        }

        return null;
    }

    private void allocateNewBuffer()
    {

        buffer = allocate();
        actualReadPosition = 0;
    }

    private void allocateNewBufferAndKeepRemainingBytes()
    {
        buffer.limit(buffer.position());
        buffer.position(actualReadPosition);
        ByteBuffer newBuffer = allocate();
        newBuffer.put(buffer);
        actualReadPosition = 0;
        buffer = newBuffer;
    }


    private boolean doDetectSize()
    {
        if(bufferedSize<4)
            return false;

        remainingSizeOfPacket = buffer.getInt();

        actualReadPosition+=4;
        bufferedSize-=4;

        if(remainingSizeOfPacket<0)
            throw new IllegalStateException("packet size is negative");

        if(remainingSizeOfPacket>0)
            return true;
        return doDetectSize();
    }

    private boolean detectSize()
    {
        if(bufferedSize<4)
            return false;

        final int positionTemp = buffer.position();

        buffer.limit(positionTemp);
        buffer.position(actualReadPosition);

        boolean b = doDetectSize();

        if(b) {
            buffer.limit(buffer.capacity());
            buffer.position(positionTemp);
        }else {
            remainingSizeOfPacket = -1;
            if(buffer.capacity()-buffer.limit()<4)
            {
                ByteBuffer newBuffer = allocate();
                newBuffer.put(buffer);
                actualReadPosition = 0;
                buffer = newBuffer;
            }else {
                buffer.limit(buffer.capacity());
                buffer.position(positionTemp);
            }
        }

        return b;
    }

    private void pushCurrentBufferToHolder()
    {
        buffer.position(actualReadPosition);
        buffer.limit(actualReadPosition+bufferedSize);
        if(buffer.hasRemaining()) {
            final int remaining  = buffer.remaining();
            remainingSizeOfPacket -= remaining;
            bufferedSize -= remaining;
            holder.addLast(buffer.duplicate().asReadOnlyBuffer());
        }
    }

}
