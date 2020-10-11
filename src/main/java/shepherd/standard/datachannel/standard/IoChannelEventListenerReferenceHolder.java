package shepherd.standard.datachannel.standard;

import shepherd.standard.assertion.Assertion;
import shepherd.standard.datachannel.IoChannel;
import shepherd.standard.datachannel.IoChannelEventListener;

import java.nio.ByteBuffer;

public class IoChannelEventListenerReferenceHolder implements IoChannelEventListener {

    private IoChannelEventListener reference;

    public IoChannelEventListenerReferenceHolder(IoChannelEventListener reference)
    {
        setReference(reference);
    }

    public IoChannelEventListenerReferenceHolder()
    {
    }


    public void setReference(IoChannelEventListener reference) {
        Assertion.ifNull("event listener is null" , reference);
        this.reference = reference;
    }

    @Override
    public void onChannelDisconnected(IoChannel ioChannel) {
        reference.onChannelDisconnected(ioChannel);
    }

    @Override
    public void onNewChannelConnected(IoChannel ioChannel) {
        reference.onNewChannelConnected(ioChannel);
    }

    @Override
    public void onDataReceived(IoChannel ioChannel, ByteBuffer[] message, byte priority) {
        reference.onDataReceived(ioChannel, message, priority);
    }

    @Override
    public void onReadRoundEnd(IoChannel ioChannel) {
        reference.onReadRoundEnd(ioChannel);
    }

    @Override
    public void onWriteRoundEnd(IoChannel ioChannel) {
        reference.onWriteRoundEnd(ioChannel);
    }
}
