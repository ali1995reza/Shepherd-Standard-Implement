package shepherd.runtime;

import shepherd.api.config.ConfigurationKey;
import shepherd.api.logger.LoggerFactory;
import shepherd.api.message.MessageMetadata;
import shepherd.api.message.MessageService;
import shepherd.standard.cluster.node.MessageServiceConfiguration;
import shepherd.standard.cluster.node.NodeConfigurations;
import shepherd.standard.cluster.node.NodeSocketAddress;
import shepherd.standard.cluster.node.StandardNode;
import shepherd.standard.datachannel.standard.StandardDataChannelConfigurations;
import shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSizeController;
import shepherd.standard.datachannel.standard.iobatch.unpooled.IoBatchConfigs;
import shepherd.standard.datachannel.standard.iobatch.unpooled.ManagedBufferSizeController;
import shepherd.standard.logger.console.ConsoleLogger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;


public class Helper {


    private final static String lineCls;

    static {
        StringBuffer buffer = new StringBuffer();

        for(int i=0;i<1000;i++)
        {
            buffer.append("\b");
        }

        lineCls = buffer.toString();
    }

    private static void waitForEver() throws InterruptedException {
        Object o = new Object();
        synchronized (o){
            o.wait();
        }
    }




    public static void printAllThreads()
    {
        for(Thread t:Thread.getAllStackTraces().keySet())
        {
            System.err.println(t);
        }
    }


    private static MessageMetadata meta;
    private static int numberOfTotalSent = 0;
    public static void main(String[] args) throws Exception
    {
        LoggerFactory.setLoggerFactory(
                new ConsoleLogger.Factory()
                .setLoggerConfigure(
                        ConsoleLogger.FULL_LOG_CONFIG
                )
        );


        StandardNode node = joinCluster(pickAFreePort() , 3030 );


        printAllThreads();
        System.out.println("NODE JOINT ");
        printAllThreads();
        node.configurations().subConfiguration("MessageService").set(NodeConfigurations.NUMBER_OF_MESSAGE_EVENT_HANDLER_THREADS , 4);


        final ManagedBufferSizeController sizeController =
                new ManagedBufferSizeController();

        sizeController.setParameters(
                new ManagedBufferSizeController.Parameters(
                        100*1024 ,
                        100*1024  ,
                        true ,
                        false
                ) ,
                BufferSizeController.Section.WRITE_AND_BATCH ,
                BufferSizeController.Section.READ_BATCH_AND_CHUNK
        );

        node.configurations().subConfiguration("StandardIoConfig")
                .subConfiguration("IoBatch")
                .set(
                        IoBatchConfigs.BUFFER_SIZE_CONTROLLER ,
                        sizeController
                );


        System.out.println(node.configurations());

        Map<ConfigurationKey, Object> conf = new HashMap<>();

        conf.put(MessageServiceConfiguration.PRIORITY , 1);
        MessageService<ByteBuffer[]> service = node.messageServiceManager().registerService(100 ,
                new NodeTest.BL());

        new ThroughputCalculator(
                node.cluster().schema()
                .nodes()
                .get(1)
        ).forEachInNewThread(
                new Consumer<ThroughputCalculator.Throughput>() {
                    @Override
                    public void accept(ThroughputCalculator.Throughput throughput) {
                        System.out.print(lineCls);
                        System.out.print(throughput.toStringMB());

                    }
                }
        );

        Thread.sleep(1000);
        new ProducerThread(
                service ,
                500 ,
                50000000 ,
                50000000).setListener(
                        t-> {

                            if (t == -1) {
                                sizeController.setParameters(
                                        new ManagedBufferSizeController.Parameters(
                                                100 * 1024,
                                                100 * 1024,
                                                true,
                                                false
                                        ),
                                        BufferSizeController.Section.WRITE_AND_BATCH,
                                        BufferSizeController.Section.READ_BATCH_AND_CHUNK
                                );
                            }
                        }
        ).run();
        waitForEver();


        conf.put(MessageServiceConfiguration.PRIORITY , 5);
        service = node.messageServiceManager().registerService(101 ,
                new NodeTest.BL() , conf);

        new Thread(new ProducerThread(
                service ,
                10 ,
                10000000 ,
                1000000)).start();
        new Thread(new ProducerThread(
                service ,
                50 ,
                10000000 ,
                1000000)).start();
        new Thread(new ProducerThread(
                service ,
                50 ,
                10000000 ,
                1000000)).start();
        new Thread(new ProducerThread(
                service ,
                50 ,
                10000000 ,
                1000000)).start();



        /*for(int i=0;i<5;i++)
        {
            conf.put(MessageServiceConfiguration.PRIORITY , i+1);
            MessageService<ByteBuffer[]> service = node.messageServiceManager().registerService(100+i ,
                    new shepherd.runtime.NodeTest.BL() , conf);
            new Thread(new shepherd.runtime.ProducerThread(
                    service ,
                    50 ,
                    40000000 ,
                    1000000)).start();
        }*/

        waitForEver();


    }

    private static int pickAFreePort()
    {
        return pickAFreePort(2000, 65535);
    }

    private static int pickAFreePort(int start , int end)
    {
        if(start<1024)
            throw new IllegalStateException("start can not less than 1024");

        if(end>65535)
            throw new IllegalStateException("maximum port number is 65535");

        int port = -1;

        for(int i=start;i<end;i++)
        {
            try{
                Socket s = new Socket();
                s.bind(new InetSocketAddress(i));
                s.close();
                port = i;
                break;
            }catch (IOException e)
            {
            }
        }

        if(port==-1)
            throw new IllegalStateException("can not pick free port");

        return port;
    }

    private static StandardNode joinCluster(int port , int jPort) throws Exception {
        StandardNode node = new StandardNode();
        node.configurations().subConfiguration("StandardIoConfig").set(StandardDataChannelConfigurations.NUMBER_OF_IO_THREADS ,1);
        /*node.configurations().subConfiguration("StandardIoConfig").set(StandardDataChannelConfigurations.SSL_CONTEXT ,
                shepherd.runtime.SSLContextBuilder.build("E:\\newCert.pfx" ,
                        "ecomotive.ir"));*/
        node.configurations().set(NodeConfigurations.NODE_ADDRESS , new InetSocketAddress(port));

        node.joinCluster(new NodeSocketAddress(new InetSocketAddress(jPort)));
        return node;
    }

    static void runAfter(int m , Runnable r)
    {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(m);
                } catch (InterruptedException e) {
                }
                r.run();
            }
        }).start();
    }
}
