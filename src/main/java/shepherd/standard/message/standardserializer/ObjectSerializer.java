package shepherd.standard.message.standardserializer;

import shepherd.utils.buffer.ByteBufferArray;
import shepherd.utils.buffer.ByteBufferArrayInputStream;
import shepherd.utils.buffer.ByteBufferInputStream;
import shepherd.utils.buffer.ExtendableByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class ObjectSerializer<T extends Serializable> extends ExceptionAdaptedSerializer<T>{


    //public final static ObjectSerializer DEFAULT = new ObjectSerializer<>(false);



    private boolean sync;
    private ExtendableByteBufferInputStream bufferInputStream;

    public ObjectSerializer(boolean synchronizedHandler)
    {
        sync = synchronizedHandler;
        if(sync)
        {
            bufferInputStream = new ExtendableByteBufferInputStream();
        }
    }


    public ObjectSerializer()
    {
        this(true);
    }


    @Override
    ByteBuffer[] doSerialize(T msg) throws Throwable {

        ByteArrayOutputStream arrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(
                arrayStream
        );
        objectStream.writeObject(msg);
        objectStream.close();
        byte[] data = arrayStream.toByteArray();
        return new ByteBuffer[]{ByteBuffer.wrap(data)};
    }

    @Override
    T doDeserialize(ByteBuffer[] rawData) throws Throwable {
        if(sync)
        {
            if(rawData.length==0)
                return null;

            bufferInputStream.setDataToRead(rawData);
            ObjectInputStream input = new ObjectInputStream(bufferInputStream);
            Object o = input.readObject();
            input.close();
            return (T)o;
        }else {
            ObjectInputStream stream = null;
            if (rawData.length == 1) {
                stream = new ObjectInputStream(new ByteBufferInputStream(rawData[0]));
            } else if (rawData.length == 0) {
                return null;
            } else {
                stream = new ObjectInputStream(
                        new ByteBufferArrayInputStream(
                                new ByteBufferArray(rawData)
                        )
                );
            }
            Object o = stream.readObject();
            stream.close();
            return (T) o;
        }
    }


}
