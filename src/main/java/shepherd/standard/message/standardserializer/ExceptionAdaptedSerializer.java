package shepherd.standard.message.standardserializer;

import shepherd.api.message.MessageSerializer;
import shepherd.api.message.exceptions.SerializeException;

import java.nio.ByteBuffer;

abstract class ExceptionAdaptedSerializer<T> implements MessageSerializer<T> {

    @Override
    public final ByteBuffer[] serialize(T msgData) throws SerializeException{
        try {
            return doSerialize(msgData);
        }catch (Throwable e)
        {
            throw new SerializeException(e);
        }
    }

    @Override
    public final T deserialize(ByteBuffer[] rawData) throws SerializeException {
        try {
            return doDeserialize(rawData);
        }catch (Throwable e)
        {
            throw new SerializeException(e);
        }
    }


    abstract ByteBuffer[] doSerialize(T msg) throws Throwable;
    abstract T doDeserialize(ByteBuffer[] rawData) throws Throwable;


}
