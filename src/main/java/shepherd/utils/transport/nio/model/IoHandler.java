package shepherd.utils.transport.nio.model;

import java.io.IOException;
import java.nio.channels.SelectableChannel;

public interface IoHandler {


    void onRegister(IoContext context, IoState state);
    void onCanceled(IoContext context);
    void read(IoContext context) throws IOException;
    void write(IoContext context) throws IOException;
    void accept(IoContext context) throws IOException;
    void connect(IoContext context) throws IOException;
    void onIdle(IoContext context, IoOperation operation) throws IOException;



    void onException(Throwable e, IoOperation op);


    SelectableChannel channel();


}
