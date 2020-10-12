package shepherd.runtime;

import shepherd.api.asynchronous.AsynchronousResultListener;
import shepherd.api.config.ConfigurationKey;
import shepherd.api.logger.LoggerFactory;
import shepherd.api.message.Message;
import shepherd.api.message.MessageListener;
import shepherd.api.message.MessageService;
import shepherd.api.message.Question;
import shepherd.api.message.exceptions.MessageException;
import shepherd.standard.cluster.node.MessageServiceConfiguration;
import shepherd.standard.cluster.node.NodeConfigurations;
import shepherd.standard.cluster.node.NodeSocketAddress;
import shepherd.standard.cluster.node.StandardNode;
import shepherd.standard.datachannel.standard.StandardDataChannelConfigurations;
import shepherd.standard.datachannel.standard.iobatch.unpooled.BufferSize;
import shepherd.standard.datachannel.standard.iobatch.unpooled.FixedLenBufferController;
import shepherd.standard.logger.console.ConsoleLogger;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeTest {


    private static void runAfter(int millisec , Runnable r)
    {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(millisec);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                r.run();
            }
        }).start();
    }

    static String msg = "djksahdkjsahdkjashdkjasndjkashdklmasdlkjasdsdsadasdsadasdasdasdasdasdasda";
    static class StringListener implements MessageListener<String>
    {

        private Random random = new Random();
        private long start = -1;
        private long end = -1;
        private int numberOfMessages = 0;
        @Override
        public void onMessageReceived(Message<String> message) {
            if(message.metadata().id()==Integer.MIN_VALUE)
            {
                latch.countDown();
            }

            if(!message.data().equals(msg))System.err.println("ERROR REALLT FIX PLEASE !");

            if(++numberOfMessages%1000000==0)
            {
                System.out.println(numberOfMessages);
            }

        }

        private static AtomicInteger a = new AtomicInteger(0);

        @Override
        public void onQuestionAsked(Question<String> question) {

        }

        private String answer()
        {
            if(random.nextInt()%2==0)
            {
                return "NO";
            }else
            {
                return "YES";
            }
        }
    }


    final static CountDownLatch latch = new CountDownLatch(1);
    final static class BL implements MessageListener<ByteBuffer[]> {


        boolean needToCall = true;
        @Override
        public void onMessageReceived(Message<ByteBuffer[]> message) {
            //System.out.println("MSG COUNT : "+(message.metadata().id()-Integer.MIN_VALUE+1));

            if(needToCall)
            {
                latch.countDown();
                needToCall = false;
            }

        }

        @Override
        public void onQuestionAsked(Question<ByteBuffer[]> question) {
            try {
                //System.out.println(question.metadata().id());
                question.response(new ByteBuffer[]{ByteBuffer.allocate(5)} , AsynchronousResultListener.EMPTY);
            } catch (MessageException e) {
                e.printStackTrace();
            }
        }
    }

    final static class OL implements MessageListener<Object> {

        @Override
        public void onMessageReceived(Message<Object> message) {
            System.out.println("MSG COUNT : "+(message.metadata().id()-Integer.MIN_VALUE+1));
        }

        @Override
        public void onQuestionAsked(Question<Object> question) {

        }
    }




    public static void main(String ... args) throws Exception
    {
        LoggerFactory.setLoggerFactory(
                new ConsoleLogger.Factory()
                .setLoggerConfigure(
                        ConsoleLogger.FULL_LOG_CONFIG_WITHOUT_STACK_TRACE
                )
        );

        List list = new ArrayList();
        StandardNode node = createCluster(3030);

        HashMap<ConfigurationKey, Object> conf = new HashMap<>();
        conf.put(MessageServiceConfiguration.PRIORITY , 5);




        node.configurations().set("MessageService/number.of.message.event.handler.threads" , 4);

        node.configurations().set("StandardIoConfig/IoBatch/buffer.size.controller" ,
                        new FixedLenBufferController(BufferSize.MB.toByte())
                );

        System.out.println(node.configurations());

        list.add(node);


        /*for(int i=0;i<0;i++) {

            Node node1 = joinCluster(3031+i , 3030);
            service = node1.messageServiceManager().registerService(10 ,new BL());
            service.synchronizedClusterEvent();
            list.add(node1);
            list.add(service);
        }*/

        System.out.println(list.size());

        //waitForEver();
        conf.put(MessageServiceConfiguration.PRIORITY , 1);
        MessageService<ByteBuffer[]> service = node.messageServiceManager().registerService(100 ,
                new BL());

        waitForEver();

        latch.await();

        for(int i=0;i<5;i++)
        {
            if(i>0)
            {
                conf.put(MessageServiceConfiguration.PRIORITY , i+1);
                service = node.messageServiceManager().registerService(100+i ,
                        new BL() , conf);

            }
            new Thread(new ProducerThread(
                    service ,
                    50 ,
                    40000000 ,
                    1000000)).start();
        }

        waitForEver();
    }

    private static void waitForEver() throws InterruptedException {
        Object o = new Object();
        synchronized (o)
        {
            o.wait();
        }
    }

    private static StandardNode createCluster(int port) throws Exception {
        StandardNode node = new StandardNode();
        node.configurations().set(NodeConfigurations.NODE_ADDRESS , new InetSocketAddress(port));
        node.configurations().subConfiguration("StandardIoConfig").set(StandardDataChannelConfigurations.NUMBER_OF_IO_THREADS ,1);
        /*node.configurations().subConfiguration("StandardIoConfig").set(StandardDataChannelConfigurations.SSL_CONTEXT ,
                shepherd.runtime.SSLContextBuilder.build("E:\\newCert.pfx" ,
                        "ecomotive.ir"));*/
        node.createCluster();
        return node;
    }

    private static StandardNode joinCluster(int port , int jPort) throws Exception {
        StandardNode node = new StandardNode();
        node.configurations().set(NodeConfigurations.NODE_ADDRESS , new InetSocketAddress(port));
        node.configurations().subConfiguration("StandardIoConfig").set(StandardDataChannelConfigurations.NUMBER_OF_IO_THREADS ,1);
        //node.configurations().subConfiguration("StandardIoConfig").set(StandardDataChannelConfigurations.SSL_CONTEXT , SSLContext.getDefault());
        //node.cluster().clusterEvent().addClusterEventListener(evli);
        node.joinCluster(new NodeSocketAddress(new InetSocketAddress(jPort)));
        return node;
    }
}
