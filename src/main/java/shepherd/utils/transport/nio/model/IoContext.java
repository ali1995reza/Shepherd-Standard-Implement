package shepherd.utils.transport.nio.model;

public interface IoContext {

    IoProcessor processor();


    <T> T setAttribute(Object key, Object value);
    <T> T removeAttribute(Object key);
    <T> T getAttribute(Object key);


}
