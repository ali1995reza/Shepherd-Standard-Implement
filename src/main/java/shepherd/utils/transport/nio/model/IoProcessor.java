package shepherd.utils.transport.nio.model;

import java.io.IOException;
import java.util.List;

public interface IoProcessor {


    void registerIoHandler(IoHandler handler) throws IOException;

    IoContext ioContext();

    boolean isInOneOfIoThreads();

    void start();

    List<IoHandler> stop();


}
