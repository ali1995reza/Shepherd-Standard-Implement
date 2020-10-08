package shepherd.standard.cluster.node;

import shepherd.standard.config.ConfigChangeResult;
import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.config.ConfigurationChangeResult;
import shepherd.api.config.ConfigurationKey;
import shepherd.api.config.IConfiguration;

import java.nio.channels.CancelledKeyException;
import java.util.concurrent.CountDownLatch;

class AcknowledgeSender {

    private int ackSendInterval;
    private Thread mainThread;
    private boolean stopped = false;
    private boolean active = false;
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final Object _sync = new Object();
    private NodeInfo currentNode;
    private NodesListManager nodesList;
    private StandardNode node;

    AcknowledgeSender(StandardNode node)
    {
        mainThread = new Thread(this::mainLoop);
        mainThread.setPriority(Thread.MAX_PRIORITY);
        mainThread.setDaemon(true);
        this.currentNode = node.info();
        nodesList = node.nodesList();
        this.node = node;
        node.configurations().
                createSubConfiguration("AcknowledgeSender").
                defineConfiguration(NodeConfigurations.ACKNOWLEDGE_SEND_INTERVAL , 5 , this::approveConfigChange);
    }



    public void setAckSendInterval(int ackSendInterval) {
        this.ackSendInterval = ackSendInterval;
    }


    private final void mainLoop()
    {
        while (active) {
            try {
                Thread.sleep(ackSendInterval);
            } catch (InterruptedException e) {
                break;
            }

            for(NodeInfoImpl info: nodesList.immutableList())
            {
                if(info==currentNode)
                    continue;

                try {
                    info.sendAck();
                }catch (CancelledKeyException e)
                {
                    //todo just go kido !
                }catch (Throwable e)
                {
                    e.printStackTrace();
                }
            }
        }

        stopLatch.countDown();
    }

    void start()
    {
        if(1==1) return;
        synchronized (_sync) {
            assertIfStopped("this acknowledge sender is stopped");
            assertIfActive("this acknowledge sender already active");
            initByConfig();
            active = true;
            mainThread.start();
        }
    }


    private void initByConfig()
    {
        ackSendInterval = node.configurations().get(NodeConfigurations.ACKNOWLEDGE_SEND_INTERVAL);

    }

    void stop()
    {
        synchronized (_sync)
        {
            assertIfStopped("this acknowledge sender is stopped");
            stopped = true;
            active = false;
            mainThread.interrupt();
        }

        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            while (stopLatch.getCount()>0);
        }
    }

    private void assertIfStopped(String msg)
    {
        if(stopped)
            throw new IllegalStateException(msg);
    }

    private void assertIfActive(String msg)
    {
        if(active)
            throw new IllegalStateException(msg);
    }


    private ConfigurationChangeResult approveConfigChange(IConfiguration parent , ConfigurationKey confName , Object current , Object val)
    {
        synchronized (_sync) {
            if (val instanceof Integer) {
                int i = (Integer) val;
                if (i > 0) {
                    setAckSendInterval(i);
                    return ConfigChangeResult.newSuccessResult(i);
                }
            }

            return ConfigChangeResult.newFailResult("Value is not instance of Integer");
        }
    }


}
