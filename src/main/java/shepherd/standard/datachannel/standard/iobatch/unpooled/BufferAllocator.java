package shepherd.standard.datachannel.standard.iobatch.unpooled;

import shepherd.standard.assertion.Assertion;
import shepherd.standard.datachannel.standard.BufferOption;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class BufferAllocator {

    private BufferOption option;
    private List<Consumer<BufferAllocator>> optionChangedListeners;

    public BufferAllocator(BufferOption option) {
        optionChangedListeners = new ArrayList<>();
        setOption(option);
    }

    public synchronized BufferAllocator addListeners(Consumer<BufferAllocator> listener)
    {
        if(!optionChangedListeners.contains(listener))
            optionChangedListeners.add(listener);
        return this;
    }

    public synchronized BufferAllocator removeListener(Consumer<BufferAllocator> listener)
    {
        optionChangedListeners.remove(listener);
        return this;
    }

    private void callListeners(){
        for(Consumer<BufferAllocator> listener : optionChangedListeners)
        {
            try {
                listener.accept(this);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }

    public synchronized void setOption(BufferOption option) {
        Assertion.ifNull("option is null", option);
        this.option = option;
        callListeners();
    }

    public BufferOption option() {
        return option;
    }

    public ByteBuffer allocate()
    {
        return BufferOption.allocate(option);
    }
}
