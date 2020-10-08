package shepherd.standard.message.standardserializer;

import java.nio.ByteBuffer;

public class ByteBufferSerializer extends ExceptionAdaptedSerializer<ByteBuffer> {


    private boolean useSourceBufferIfPossible;
    private boolean direct;

    public ByteBufferSerializer(boolean useSourceBufferIfPossible , boolean direct)
    {
        this.useSourceBufferIfPossible = useSourceBufferIfPossible;
        this.direct = direct;
    }

    public ByteBufferSerializer(boolean useSourceBufferIfPossible)
    {
        this(useSourceBufferIfPossible , false);
    }

    public ByteBufferSerializer()
    {
        this(true , false);
    }



    @Override
    ByteBuffer[] doSerialize(ByteBuffer msg) throws Throwable {
        return new ByteBuffer[]{msg};
    }

    @Override
    ByteBuffer doDeserialize(ByteBuffer[] rawData) throws Throwable {

        if(useSourceBufferIfPossible)
        {
            if(rawData.length==1)
            {
                return rawData[0];
            }else if(rawData.length==0)
            {
                return allocate(0);
            }else
            {
                byte condition = 0;
                ByteBuffer lastBuffer = null;
                for(ByteBuffer b:rawData)
                {
                    if(b.hasRemaining())
                    {
                        lastBuffer =b;
                        if(++condition>1)
                            break;
                    }
                }

                if(condition==0)
                    return allocate(0);
                else if (condition==1)
                    return lastBuffer;
            }
        }


        int size = 0;
        for(ByteBuffer b:rawData)
        {
            size+=b.remaining();
        }

        ByteBuffer buffer = allocate(size);

        for(ByteBuffer b:rawData)
        {
            buffer.put(b);
        }

        buffer.flip();

        return buffer;
    }


    private ByteBuffer allocate(int size)
    {
        if(direct)
            return ByteBuffer.allocateDirect(size);

        return ByteBuffer.allocate(size);
    }
}
