package shepherd.runtime;

import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.cluster.node.NodeStatistics;

import java.util.function.Consumer;

public class ThroughputCalculator {

    public final static class Throughput {


        private final NodeStatistics statistics;
        private final float dv;

        private long lastReceivedBytes;
        private long lastSentBytes;
        private long lastReceivedMessages;
        private long lastSentMessages;



        public Throughput(NodeStatistics statistics , float dv) {
            this.statistics = statistics;
            this.dv = dv;
        }

        private void capture()
        {
            lastReceivedBytes = statistics.totalReceivedBytes();
            lastSentBytes = statistics.totalSentBytes();
            lastReceivedMessages = statistics.totalReceivedPackets();
            lastSentMessages = statistics.totalSentPackets();
        }

        public float sendBytesPerSeconds()
        {
            return (statistics.totalSentBytes()-lastSentBytes)/dv;
        }

        public float receiveBytesPerSeconds()
        {
            return (statistics.totalReceivedBytes()-lastReceivedBytes)/dv;
        }

        public float sendMessagesPerSeconds()
        {
            return (statistics.totalSentPackets()-lastSentMessages)/dv;
        }


        public float receiveMessagesPerSeconds()
        {
            return (statistics.totalReceivedPackets()-lastReceivedMessages)/dv;
        }


        public String toStringKB() {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("Throughput : Send -> ")
                    .append(sendBytesPerSeconds()/1024)
                    .append(" KB/s , ")
                    .append(sendMessagesPerSeconds())
                    .append(" Packet/s")
                    .append(" - ")
                    .append("Recv -> ")
                    .append(receiveBytesPerSeconds()/1024)
                    .append(" KB/s , ")
                    .append(receiveMessagesPerSeconds())
                    .append(" Packet/s");

            return stringBuffer.toString();
        }



        public String toStringMB() {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("Throughput : Send -> ")
                    .append(sendBytesPerSeconds()/(1024*1024))
                    .append(" MB/s , ")
                    .append(sendMessagesPerSeconds())
                    .append(" Packet/s")
                    .append(" - ")
                    .append("Recv -> ")
                    .append(receiveBytesPerSeconds()/(1024*1024))
                    .append(" MB/s , ")
                    .append(receiveMessagesPerSeconds())
                    .append(" Packet/s");

            return stringBuffer.toString();
        }

        @Override
        public String toString() {
           return toStringKB();
        }
    }




    private final static void sleep(long l)
    {
        try {
            Thread.sleep(l);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }


    private final NodeInfo nodeInfo;
    private final int averageOfHowManySeconds;

    public ThroughputCalculator(NodeInfo nodeInfo, int averageOfHowManySeconds) {
        this.nodeInfo = nodeInfo;
        this.averageOfHowManySeconds = averageOfHowManySeconds;
    }

    public ThroughputCalculator(NodeInfo nodeInfo) {
        this(nodeInfo , 1);
    }


    public void forEach(Consumer<Throughput> forEach)
    {
        Throughput throughput = new Throughput(nodeInfo.statistics(), averageOfHowManySeconds);


        while (true) {
            throughput.capture();
            sleep(averageOfHowManySeconds * 1000);
            forEach.accept(throughput);
        }

    }


    public void forEachInNewThread(Consumer<Throughput> forEach)
    {
        new Thread(new Runnable() {
            @Override
            public void run() {
                forEach(forEach);
            }
        }).start();
    }

}
