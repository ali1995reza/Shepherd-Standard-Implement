package shepherd.utils.transport.nio.model;

import java.nio.channels.SelectionKey;

public enum IoOperation {

    ACCEPT((byte)SelectionKey.OP_ACCEPT) ,
    READ((byte)SelectionKey.OP_READ) ,
    WRITE((byte)SelectionKey.OP_WRITE) ,
    CONNECT((byte)SelectionKey.OP_CONNECT) ,
    CANCEL((byte)-1) ,
    IDLE((byte)-2);


    public final byte code;
    public final byte MASK;

    IoOperation(byte c)
    {
        this.code = c;
        MASK = (byte)(code&Byte.MAX_VALUE);
    }

}
