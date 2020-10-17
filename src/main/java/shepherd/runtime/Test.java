package shepherd.runtime;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Test {

    public static void main(String[] args) throws Exception
    {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(3030));
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0);
        buffer.clear();
        while (true)
        {
            socket.getOutputStream().write(
                 buffer.array()
            );
            Thread.sleep(500);
        }
    }
}
