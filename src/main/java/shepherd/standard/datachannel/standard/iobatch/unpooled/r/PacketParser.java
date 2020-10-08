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
            if(bufferedSize>=4)
            {
                detectSize();
                if(remainingSizeOfPacket>0)
                {
                    return chunkBuffer();
                }else if(remainingSizeOfPacket==0)
                {
                    remainingSizeOfPacket = -1;
                    return chunkBuffer();
                }else
                {
                    throw new IOException("packet size is negative");
                }
            }
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



    private void detectSize()
    {
        final int positionTemp = buffer.position();
        buffer.limit(positionTemp);
        buffer.position(actualReadPosition);

        remainingSizeOfPacket = buffer.getInt();


        actualReadPosition+=4;
        bufferedSize-=4;

        buffer.limit(buffer.capacity());
        buffer.position(positionTemp);
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
