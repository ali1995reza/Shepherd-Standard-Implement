package shepherd.standard.cluster.node;

import shepherd.standard.datachannel.IoChannel;
import shepherd.utils.buffer.ByteBufferArray;

final class ClusterLevelEvent
{
    enum Type
    {
        DATA_RECEIVED , CONNECT , DISCONNECT;

        boolean is(Type t)
        {
            return this == t;
        }
        boolean isNot(Type t) { return  this!=t; }
    }


    final static ClusterLevelEvent disconnectEvent(IoChannel c)
    {
        return new ClusterLevelEvent(c , Type.DISCONNECT);
    }

    final static ClusterLevelEvent connectEvent(IoChannel c)
    {
        return new ClusterLevelEvent(c , Type.CONNECT);
    }


    final static ClusterLevelEvent dataEvent(IoChannel c , ByteBufferArray data)
    {
        return new ClusterLevelEvent(c , data);
    }

    private IoChannel ioChannel;
    private ByteBufferArray data;
    private Type type;

    public ClusterLevelEvent(IoChannel c , Type t , ByteBufferArray d)
    {
        ioChannel = c;
        type = t;
        data = d;
    }

    public ClusterLevelEvent(IoChannel c , ByteBufferArray d)
    {
        this(c , Type.DATA_RECEIVED , d);
    }

    public ClusterLevelEvent(IoChannel c , Type t)
    {
        this(c , t , null);
    }


    public ByteBufferArray data() {
        return data;
    }

    public Type type() {
        return type;
    }

    public boolean is(Type t)
    {
        return type==t;
    }

    public IoChannel channel() {
        return ioChannel;
    }
}