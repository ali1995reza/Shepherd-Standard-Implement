package shepherd.utils.buffer;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class Utils {



    public static ByteBuffer[] duplicate(ByteBuffer[] original)
    {
        ByteBuffer[] dup = new ByteBuffer[original.length];
        for(int i=0;i<original.length;i++)
        {
            dup[i] = original[i].duplicate();
        }
        return dup;
    }

    public static ByteBuffer[] duplicateAsOnlyReadable(ByteBuffer[] original)
    {
        ByteBuffer[] dup = new ByteBuffer[original.length];
        for(int i=0;i<original.length;i++)
        {
            dup[i] = original[i].duplicate().asReadOnlyBuffer();
        }

        return dup;
    }

    public static ByteBuffer[] combine(ByteBuffer[] ... arrays)
    {
        int size = 0;
        for(ByteBuffer[] array:arrays)
        {
            size+=array.length;
        }

        ByteBuffer[] combined = new ByteBuffer[size];

        int count = 0;

        for(ByteBuffer[] array:arrays)
        {
            for(ByteBuffer buffer:array)
            {
                combined[count++]  = buffer;
            }
        }

        return combined;
    }


    public static ByteBuffer[] combine(ByteBuffer header  , ByteBuffer[] ... arrays)
    {
        int size = 0;
        for(ByteBuffer[] array:arrays)
        {
            size+=array.length;
        }

        ByteBuffer[] combined = new ByteBuffer[size+1];

        int count = 0;
        combined[count++] = header;

        for(ByteBuffer[] array:arrays)
        {
            for(ByteBuffer buffer:array)
            {
                combined[count++]  = buffer;
            }
        }

        return combined;
    }


    public static int getInt(ByteBuffer[] buffers)
    {
        if(buffers.length==1)
        {
            return buffers[0].getInt();

        }else
        {
            byte i = 3;
            int integer = 0;

            ByteBuffer b;
            for(int index=0;index<buffers.length;index++)
            {

                b = buffers[index];
                while (b.hasRemaining())
                {
                    integer |= (b.get()&0xff) << ((i--) * 8);

                    if(i==-1)
                        return integer;
                }

            }

            throw new BufferUnderflowException();
        }
    }


    public static long getLong(ByteBuffer[] buffers)
    {

        if(buffers.length==1)
        {
            return buffers[0].getLong();
        }else
        {
            byte i = 7;
            long lng = 0;

            ByteBuffer b;
            for(int index=0;index<buffers.length;index++)
            {
                b = buffers[index];
                while (b.hasRemaining())
                {
                    lng |= (b.get()&0xff) << ((i--) * 8);

                    if(i==-1)
                        return lng;
                }

            }

            throw new BufferUnderflowException();
        }
    }


    public static byte getByte(ByteBuffer[] buffers)
    {
        if(buffers.length==1)
        {
            return buffers[0].get();
        }else {
            ByteBuffer b;
            for (int index=0;index<buffers.length;index++) {
                b = buffers[index];
                if (b.hasRemaining())
                    return b.get();
            }
        }

        throw new BufferUnderflowException();
    }

    public static byte[] getRemainingDataAsArray(ByteBuffer[] buffers)
    {
        int size = 0;
        for(int index=0;index<buffers.length;index++)
        {
            size+=buffers[index].remaining();
        }

        byte[] array = new byte[size];

        int arrayIndex = 0;

        ByteBuffer b;

        for(int index=0;index<buffers.length;index++)
        {
            b = buffers[index];
            while (b.hasRemaining())
            {
                array[arrayIndex++] = b.get();
            }
        }

        return array;
    }

}
