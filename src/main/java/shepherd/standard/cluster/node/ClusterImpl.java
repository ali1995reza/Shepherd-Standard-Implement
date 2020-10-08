package shepherd.standard.cluster.node;

import shepherd.standard.config.Configuration;
import shepherd.api.cluster.ClusterEvent;
import shepherd.api.cluster.ClusterSchema;
import shepherd.api.cluster.ClusterState;
import shepherd.api.config.IConfiguration;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

class ClusterImpl implements shepherd.api.cluster.Cluster {

    private RuntimeMXBean mxBean;
    private long timeOffset = -1;
    private IConfiguration configuration;
    private ClusterState state = ClusterState.UNKNOWN;
    private final ClusterEventImpl clusterEvent;
    private final Object _sync = new Object();
    private final NodesListManager nodesList;

    ClusterImpl(NodesListManager listManager)
    {
        mxBean = ManagementFactory.getRuntimeMXBean();
        nodesList = listManager;
        configuration = new Configuration("ClusterConfig");
        clusterEvent = new ClusterEventImpl(true);
    }



    int nextID()
    {
        for(int i=1;i<Integer.MAX_VALUE;i++)
        {
            if(nodesList.fastFindById(i)==null)
                return i;
        }

        throw new IllegalStateException();
    }




    NodeInfoImpl leader()
    {

        return nodesList.leaderInfoImpl();
    }




    public void setClusterPointTime(long clusterTime)
    {
        long current = mxBean.getUptime();
        timeOffset = clusterTime-current;
        timeOffset = timeOffset<0?0:timeOffset;

    }

    public void setLocalClusterPointTime()
    {
        timeOffset = 0;
    }

    @Override
    public ClusterSchema schema() {
        return nodesList;
    }

    @Override
    public ClusterEvent clusterEvent() {

        return clusterEvent;
    }


    ClusterEventImpl clusterEventImpl()
    {
        return clusterEvent;
    }

    @Override
    public long clusterTime() {
        if(timeOffset==-1)
            throw new IllegalStateException("cluster time did not synchronize yet");

        return mxBean.getUptime()+timeOffset;
    }

    @Override
    public IConfiguration clusterConfig() {
        return configuration;
    }

    void setState(ClusterState state)
    {
        synchronized (_sync) {
            ClusterState lastState = this.state;
            this.state = state;
            clusterEvent.notifyClusterStateChanged(lastState , this.state);
        }
    }

    @Override
    public ClusterState state() {
        return state;
    }


    public void stopAllServices()
    {
        clusterEvent.stop();
    }
}
