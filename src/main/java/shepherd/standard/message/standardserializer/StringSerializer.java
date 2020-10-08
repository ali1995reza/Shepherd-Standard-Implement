package shepherd.standard.message.standardserializer;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

public class StringSerializer extends ExceptionAdaptedSerializer<String> {

    private final Charset charset;
    private final CharsetDecoder decoder;

    public StringSerializer(String charset)
    {
        this(Charset.forName(charset));
    }

    public StringSerializer(Charset charset)
    {

        this.charset = charset;
        decoder = charset.newDecoder();
    }

    public StringSerializer()
    {
        this(Charset.forName("UTF-8"));
    }


    @Override
    ByteBuffer[] doSerialize(String msg) {
        return new ByteBuffer[]{charset.encode(msg)};
    }

    @Override
    String doDeserialize(ByteBuffer[] rawData) throws Throwable {

        int size = 0;


        for(ByteBuffer b:rawData)
        {
            size+=b.remaining();
        }


        CharBuffer buffer = CharBuffer.allocate(size);


        CoderResult result = null;
        for(int i=0;i<rawData.length;i++)
        {

            ByteBuffer b = rawData[i];
            if(i==rawData.length-1) {
                result = decoder.decode(b, buffer, true);
            }else
            {
                result = decoder.decode(b , buffer , false);
            }

            if(result.isUnmappable() || result.isMalformed())
            {
                decoder.reset();
                result.throwException();
            }
        }


        decoder.reset();

        buffer.flip();

        return buffer.toString();
    }
}
