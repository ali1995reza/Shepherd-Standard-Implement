package shepherd.runtime;

import shepherd.api.asynchronous.AsynchronousResultListener;
import shepherd.api.message.MessageService;
import shepherd.api.message.ack.Acknowledge;
import shepherd.standard.assertion.Assertion;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class ProducerThread implements Runnable {

    private final static char NEW_LINE = '\n';


    private final MessageService<ByteBuffer[]> service;
    private final int size;
    private final int totalMessages;
    private final int logEvery;
    private final String tag;
    private final String endTag;

    private long totalLatency = 0;

    private Consumer<Integer> listener = new Consumer<Integer>() {
        @Override
        public void accept(Integer integer) {

        }
    };

    public ProducerThread(MessageService<ByteBuffer[]> service, int size, int totalMessages, int logEvery) {
        this.service = service;
        this.size = size;
        this.totalMessages = totalMessages;
        this.logEvery = logEvery;
        this.tag = "================================ "+service.id()+" ================================";
        String s = "";
        for(int i=0;i<tag.length();i++)
        {
            s+="=";
        }
        endTag = s;

    }

    public ProducerThread setListener(Consumer<Integer> l)
    {
        Assertion.ifNull("listener can not be null" , l);

        listener = l;


        return this;

    }

    private final void callListener(int i)
    {
        try {
            listener.accept(i);
        }catch (Throwable e)
        {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        try {
            ByteBuffer d = ByteBuffer.allocate(size);

            ByteBuffer[] msg = new ByteBuffer[]{d};


            final long start = System.currentTimeMillis();

            final CountDownLatch endLatch = new CountDownLatch(1);

            final StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < totalMessages; i++) {
                d.clear();
                final int s = i + 1;
                if (s % logEvery == 0) {
                    final long sentTime = System.currentTimeMillis();
                    service.sendMessage(msg, MessageService.DefaultArguments.ALL_ACKNOWLEDGES, new AsynchronousResultListener<Acknowledge>() {
                        @Override
                        public void onUpdated(Acknowledge result) {

                        }

                        @Override
                        public void onCompleted(Acknowledge acknowledge) {
                            long end = System.currentTimeMillis();
                            long latency = end-sentTime;
                            totalLatency += latency;

                            stringBuffer.setLength(0);
                            stringBuffer
                                    .append(NEW_LINE)
                                    .append(NEW_LINE)
                                    .append(tag)
                                    .append(NEW_LINE)
                                    .append("Total Number Of Messages Until Now : ")
                                    .append(s)
                                    .append(NEW_LINE)
                                    .append("Time : ")
                                    .append(end-start)
                                    .append(NEW_LINE)
                                    .append("Latency : ")
                                    .append(latency)
                                    .append(NEW_LINE)
                                    .append(endTag);

                            System.out.println(stringBuffer.toString());

                            callListener(s);

                            if(s==totalMessages)
                                endLatch.countDown();
                        }
                    });

                    System.out.println("TIME TO BUFFER  : "+(System.currentTimeMillis()-sentTime));
                } else {
                    final long sentTime = System.currentTimeMillis();
                    service.sendMessage(msg, MessageService.DefaultArguments.ALL_ACKNOWLEDGES, new AsynchronousResultListener<Acknowledge>() {
                        @Override
                        public void onUpdated(Acknowledge result) {

                        }

                        @Override
                        public void onCompleted(Acknowledge result) {
                            long latency = System.currentTimeMillis()-sentTime;
                            totalLatency += latency;


                            if(s==totalMessages)
                                endLatch.countDown();

                        }
                    });

                    //service.sendMessage(msg , MessageService.DefaultArguments.ALL_ACKNOWLEDGES , AsynchronousResultListener.EMPTY);
                }
            }

            endLatch.await();

            stringBuffer.setLength(0);
            stringBuffer
                    .append(NEW_LINE)
                    .append(NEW_LINE)
                    .append(tag)
                    .append(NEW_LINE)
                    .append("Average Latency : ")
                    .append((totalLatency/totalMessages))
                    .append(NEW_LINE)
                    .append(endTag);

            System.out.println(stringBuffer.toString());
        }catch (Exception e)
        {
            e.printStackTrace();
            return;
        }
    }


    private final static String toString(Acknowledge acknowledge)
    {
        return new StringBuffer()
                .append("Ack {F:")
                .append(acknowledge.numberOfFailedAcks())
                .append(" , S:")
                .append(acknowledge.numberOfSuccessAcks())
                .append("}")
                .toString();
    }
}
