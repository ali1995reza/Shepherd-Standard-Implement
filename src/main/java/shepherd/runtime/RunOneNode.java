package shepherd.runtime;

public class RunOneNode {
/*


    private static int TARGET_ID = Integer.MIN_VALUE+200000000-1;

    public static MessageListener<String> listener = new MessageListener<String>() {

        private Random random = new Random();
        @Override
        public void onMessageReceived(Message<String> message) {
            if(message.metadata().id()==TARGET_ID)
            {
                System.out.println(message);
                System.out.println("TIME : "+System.currentTimeMillis());
            }
        }

        @Override
        public void onQuestionAsked(Question<String> question) {
            System.out.println("Question : " + question);
            try {
                //question.response(answer());
                //todo nothing
            } catch (Exception e) {
                e.printStackTrace();
            }

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
    };
    public static MessageListener<ByteBuffer[]> bl = new MessageListener<ByteBuffer[]>() {
        @Override
        public void onMessageReceived(Message<ByteBuffer[]> message) {
            if(message.metadata().id()==TARGET_ID)
            {
                System.out.println(message);
                System.out.println("TIME : "+System.currentTimeMillis());
            }
        }

        @Override
        public void onQuestionAsked(Question<ByteBuffer[]> question) {
            System.out.println("QUESTION : "+question);
            try {
                question.response(new ByteBuffer[]{ByteBuffer.allocate(1000)});
            } catch (MessageException e) {
                e.printStackTrace();
            }
        }
    };

    private static ClusterEventListener evli = new ClusterEventListener() {
        @Override
        public void onClusterStateChanged(ClusterState lastState, ClusterState currentState) {
            System.out.println("CLUSTER STATE CHANGED : "+lastState+" - "+currentState);
        }

        @Override
        public void onNodeStateChanged(NodeInfo node, NodeState lastState, NodeState currentState) {
            System.out.println("NODE STATE CHANGED : "+lastState +" - "+node);
        }

        @Override
        public void onLeaderChanged(NodeInfo leader) {

            System.err.println("LEADER CHANGED !!!!! , "+leader);
        }
    };


    private static void waitForEver() throws InterruptedException {
        Object o = new Object();
        synchronized (o)
        {
            o.wait();
        }
    }


    static CountDownLatch latch = new CountDownLatch(10);
    static class BL implements MessageListener<ByteBuffer[]>
    {

        @Override
        public void onMessageReceived(Message<ByteBuffer[]> message) {
            latch.countDown();
        }

        @Override
        public void onQuestionAsked(Question<ByteBuffer[]> question) {

        }
    }



    public static void main(String ... args) throws Exception
    {
        if(args==null || args.length==0)
        {
            System.out.print("Init : ");
            Scanner scanner = new Scanner(System.in);
            args = scanner.nextLine().split("\\s+");
        }

        MessageService<ByteBuffer[]> service = null;

        if(args[0].toLowerCase().equals("create")) {
            Node node = createCluster(args[1].split(":")[0].trim()  , Integer.parseInt(args[1].split(":")[1].trim()));
            service = node.messageServiceManager().registerService(10 , new BL());

        }else if(args[0].toLowerCase().equals("join"))
        {
            Node node = joinCluster(Integer.parseInt(args[1]) ,
                    Integer.parseInt(args[2]));
            service = node.messageServiceManager().registerService(10,  new BL());

        }

        waitForEver();
        latch.await();


        final int[] nodes = new int[]{1 , 2, 3, 4 , 5 , 6};
        ByteBuffer d = ByteBuffer.allocate(50);


        final long start = System.currentTimeMillis();
        for(int i=0;i<200000000;i++)
        {
            final int s = i+1;
            if(s%1000000==0) {
                final long sentTime = System.currentTimeMillis();
                service.sendMessage(new ByteBuffer[]{d.duplicate()}, nodes.length , nodes ).setListener(new AsynchronousResultListener<Acknowledge, Object>() {
                    @Override
                    public void onUpdated(Acknowledge result, Object attachment) {

                    }

                    @Override
                    public void onCompleted(Acknowledge acknowledge, Object attachment) {

                        long end = System.currentTimeMillis();
                        System.out.println(acknowledge);
                        System.out.println(s);
                        System.out.println(end-start);
                        System.out.println("AFTER SENT : "+ (end-sentTime));
                        //System.out.println("MAX : "+max);
                        //System.out.println("ALL  : "+all);
                    }
                });
                System.out.println("************ == DELAY : "+(System.currentTimeMillis()-sentTime));
                //Thread.sleep(10000000);
            }else
            {
                //final long sendTime = System.currentTimeMillis();
                service.sendMessage(new ByteBuffer[]{d.duplicate()}, nodes.length , nodes);
            }
        }
        long currentTime = System.currentTimeMillis();
        System.out.println("CURRENT TIME : "+currentTime);
        System.out.println("WHOLE SEND TIME : "+(currentTime-start));

        waitForEver();
    }


    private static Node createCluster(String host , int port ) throws IOException {
        Node node = new Node();

        node.configurations().set(NodeConfigurations.NODE_ADDRESS , new InetSocketAddress(host ,port));
        node.cluster().clusterEvent().addClusterEventListener(evli);
        node.configurations().set(StandardDataChannelConfigurations.BLOCK_WRITE_UNTIL_BUFFER_AVAILABLE , true);
        node.configurations().set(StandardDataChannelConfigurations.BUFFER_POOL_SIZE , 1024*100);
        node.configurations().set(StandardDataChannelConfigurations.BUFFER_POOL_SIZE , 200);
        node.configurations().set(NodeConfigurations.NUMBER_OF_MESSAGE_EVENT_HANDLER_THREADS , 4);
        node.createCluster();
        return node;
    }

    private static Node joinCluster(int port , int jPort ) throws IOException, JSONException {
        Node node = new Node();
        node.configurations().set(NodeConfigurations.NODE_ADDRESS , new InetSocketAddress(port));
        node.cluster().clusterEvent().addClusterEventListener(evli);
        node.configurations().set(StandardDataChannelConfigurations.BLOCK_WRITE_UNTIL_BUFFER_AVAILABLE , true);
        node.configurations().set(StandardDataChannelConfigurations.BUFFER_POOL_SIZE , 1024*100);
        node.configurations().set(NodeConfigurations.NUMBER_OF_MESSAGE_EVENT_HANDLER_THREADS , 4);
        node.joinCluster(new NodeSocketAddress(new InetSocketAddress("localhost", jPort)));
        return node;
    }

 */
}
