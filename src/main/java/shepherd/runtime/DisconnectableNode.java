package shepherd.runtime;

import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.logger.LoggerFactory;
import shepherd.standard.cluster.node.NodeConfigurations;
import shepherd.standard.cluster.node.NodeSocketAddress;
import shepherd.standard.cluster.node.StandardNode;
import shepherd.standard.datachannel.IoChannel;
import shepherd.standard.datachannel.IoChannelCenter;
import shepherd.standard.datachannel.standard.StandardDataChannelConfigurations;
import shepherd.standard.logger.console.ConsoleLogger;

import java.net.InetSocketAddress;
import java.util.Scanner;
import java.util.function.Consumer;

public class DisconnectableNode {

    private static StandardNode provide(int port , int jPort) throws Exception {
        StandardNode node = new StandardNode();
        node.configurations().subConfiguration("StandardIoConfig").set(StandardDataChannelConfigurations.NUMBER_OF_IO_THREADS ,1);
        /*node.configurations().subConfiguration("StandardIoConfig").set(StandardDataChannelConfigurations.SSL_CONTEXT ,
                shepherd.runtime.SSLContextBuilder.build("E:\\newCert.pfx" ,
                        "ecomotive.ir"));*/
        node.configurations().set(NodeConfigurations.NODE_ADDRESS , new InetSocketAddress(port));

        node.joinCluster(new NodeSocketAddress(new InetSocketAddress(jPort)));
        return node;
    }


    public static void main(String [] args)  throws Exception
    {
        LoggerFactory.setLoggerFactory(
                new ConsoleLogger.Factory()
                        .setLoggerConfigure(
                                ConsoleLogger.FULL_LOG_CONFIG_WITHOUT_STACK_TRACE
                        )
        );
        StandardNode node = provide(Helper.pickAFreePort() , 3030);

        new MaxThreadUsage().start();

        IoChannelCenter ioChannelCenter = node.getIoChannelCenter();

        String command = new Scanner(System.in).nextLine();

        Integer integer = Integer.parseInt(command);

        for(IoChannel ioChannel:ioChannelCenter.connectedChannels())
        {
            NodeInfo info = ioChannel.attachment();
            if(info.id() == integer)
            {
                ioChannel.closeNow();
                break;
            }
        }

        Thread.sleep(100000000);
    }

}
