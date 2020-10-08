package shepherd.utils.buffer;



import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class ByteBufferArray {


    private ByteBuffer[] data;
    private int index = -1;
    private int remaining = 0;


    public ByteBufferArray(ByteBuffer[] d , boolean duplicate)
    {

        if(duplicate) {
            data = new ByteBuffer[d.length];
            for (int i = 0; i < d.length; i++) {
                data[i] = d[i].duplicate();
                if(index==-1 && data[i].hasRemaining())
                {
                    index = i;
                }
                remaining += data[i].remaining();
            }
        }else
        {
            data =  d;
            for (int i = 0; i < data.length; i++) {
                if(index==-1 && data[i].hasRemaining())
                {
                    index = i;
                }
                remaining += data[i].remaining();
            }
        }
    }

    public ByteBufferArray(ByteBuffer[] d)
    {
        this(d,false);
    }



    public int getInt()
    {
        if(remaining>=4)
        {

            byte i = 3;
            int integer = 0;

            ByteBuffer b = data[index];
            while (true)
            {
                if(!b.hasRemaining()) {
                    b = data[++index];
                    continue;
                }

                integer |= (b.get()&0xff) << ((i--) * 8);
                --remaining;


                if(i==-1)
                    return integer;

            }
        }

        throw new BufferUnderflowException();
    }


    public byte get()
    {
        if(remaining>=1)
        {
            //so has data to get !
            //handleBy it please !
            byte b = data[index].get();
            --remaining;
            if(!data[index].hasRemaining())
                ++index;

            return b;
        }

        throw new BufferUnderflowException();
    }

    public long getLong()
    {
        if(remaining>=8)
        {

            byte i = 7;
            long lng = 0;

            ByteBuffer b = data[index];
            while (true)
            {

                if(!b.hasRemaining()) {
                    b = data[++index];
                    continue;
                }
                lng |= (b.get()&0xff) << ((i--) * 8);
                --remaining;


                if(i==-1)
                    return lng;

            }
        }

        throw new BufferUnderflowException();
    }

    public byte[] getArray(int l)
    {
        if(remaining>=l)
        {
            byte[] array = new byte[l];
            int arrIndex = 0;
            ByteBuffer b = data[index];
            while (b.hasRemaining())
            {
                array[arrIndex++] = b.get();
                --remaining;



                if(arrIndex==array.length)
                    return array;

                if(!b.hasRemaining())
                    b = data[++index];

            }
        }


        throw new BufferUnderflowException();
    }

    public ByteBuffer[] buffers() {
        return data;
    }

    public int remaining()
    {
        return remaining;
    }

    public boolean hasRemaining()
    {
        return remaining>0;
    }
}
