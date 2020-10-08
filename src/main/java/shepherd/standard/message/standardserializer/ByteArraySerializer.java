package shepherd.standard.message.standardserializer;

import shepherd.utils.buffer.Utils;

import java.nio.ByteBuffer;

public class ByteArraySerializer extends ExceptionAdaptedSerializer<byte[]> {

    @Override
    ByteBuffer[] doSerialize(byte[] msg) throws Throwable {
        return new ByteBuffer[]{ByteBuffer.wrap(msg)};
    }

    @Override
    byte[] doDeserialize(ByteBuffer[] rawData) throws Throwable {
        return Utils.getRemainingDataAsArray(rawData);
    }
}
