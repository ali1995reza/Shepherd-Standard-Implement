package shepherd.standard.message.standardserializer;

import shepherd.api.message.MessageSerializer;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class AppenderSerializer<T> extends ExceptionAdaptedSerializer<T> {



    private final Supplier<ByteBuffer> headerSupplier;
    private final MessageSerializer<T> serializer;

    public AppenderSerializer(Supplier<ByteBuffer> headerSupplier, MessageSerializer<T> serializer) {
        this.headerSupplier = headerSupplier;
        this.serializer = serializer;
    }

    @Override
    ByteBuffer[] doSerialize(T msg) throws Throwable {
        ByteBuffer[] serialized = serializer.serialize(msg);
        ByteBuffer header = headerSupplier.get();
        if(header==null || !header.hasRemaining())
            return serialized;

        return combine(header , serialized);
    }

    @Override
    T doDeserialize(ByteBuffer[] rawData) throws Throwable {
        return serializer.deserialize(rawData);
    }


    private final static ByteBuffer[] combine(ByteBuffer header , ByteBuffer[] content)
    {
        ByteBuffer[] buffers = new ByteBuffer[content.length+1];
        buffers[0] = header;
        for(int i=0;i<content.length;i++)
        {
            buffers[i+1] = content[i];
        }

        return buffers;
    }
}
