package shepherd.standard.cluster.node.clusterlevelmessage;

import java.lang.management.ManagementFactory;

public class TimeSync implements ClusterMessage {


    public final static TimeSync now()
    {
        return new TimeSync().setSendTime(
                ManagementFactory.getRuntimeMXBean().getUptime()
        );
    }

    private long sendTime;
    private long clusterTime;

    public long sendTime() {
        return sendTime;
    }

    public long clusterTime() {
        return clusterTime;
    }

    public TimeSync setSendTime(long sendTime) {
        this.sendTime = sendTime;
        return this;
    }

    public TimeSync setClusterTime(long clusterTime) {
        this.clusterTime = clusterTime;
        return this;
    }
}
