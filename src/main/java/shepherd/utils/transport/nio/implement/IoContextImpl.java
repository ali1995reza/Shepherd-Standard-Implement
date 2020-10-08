package shepherd.utils.transport.nio.implement;


import shepherd.utils.transport.nio.model.IoContext;
import shepherd.utils.transport.nio.model.IoProcessor;

import java.util.concurrent.ConcurrentHashMap;

class IoContextImpl implements IoContext {

    private IoProcessor processor;
    private ConcurrentHashMap attributes;

    IoContextImpl(IoProcessor p)
    {
        processor = p;
        attributes = new ConcurrentHashMap();
    }


    @Override
    public IoProcessor processor() {
        return processor;
    }


    @Override
    public <T> T setAttribute(Object key, Object value) {
        Object o = attributes.put(key , value);
        if(o==null)
            return null;

        return (T)o;
    }

    @Override
    public <T> T removeAttribute(Object key) {
        Object o = attributes.remove(key);
        if(o==null)
            return null;

        return (T)o;
    }

    @Override
    public <T> T getAttribute(Object key) {
        Object o = attributes.get(key);
        if(o==null)
            return null;

        return (T)o;
    }


}
