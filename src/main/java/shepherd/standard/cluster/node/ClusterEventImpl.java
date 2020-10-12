package shepherd.standard.cluster.node;


import shepherd.api.cluster.ClusterEvent;
import shepherd.api.cluster.ClusterEventListener;
import shepherd.api.cluster.ClusterState;
import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.cluster.node.NodeState;
import shepherd.api.logger.Logger;
import shepherd.api.logger.LoggerFactory;
import shepherd.utils.concurrency.threaddispatcher.Dispatcher;
import shepherd.utils.concurrency.threaddispatcher.simple.dispatcher.SimpleDispatcher;

import java.util.ArrayList;

final class ClusterEventImpl implements ClusterEvent {

    private ArrayList<ClusterEventListener> listeners;
    private ArrayList<ClusterEventListener> topList;
    private final Object _sync = new Object();
    private Dispatcher<Object[]> dispatcher;
    private final boolean hasOwnThread;

    private final Logger logger;

    public ClusterEventImpl(boolean hasOwnThread)
    {
        this.hasOwnThread = hasOwnThread;

        if(hasOwnThread)
            dispatcher = new SimpleDispatcher<>(this::handler);

        listeners = new ArrayList<>();
        topList = new ArrayList<>();
        logger = LoggerFactory.factory().getLogger(this);
    }


    public ClusterEventImpl()
    {
        this(true);
    }



    void start()
    {
        if(!hasOwnThread)
            return;

        if(dispatcher.isActive())
            return;
        dispatcher.start();
    }

    void stop()
    {
        if(!hasOwnThread)
            return;
        dispatcher.terminateAndWaitToFinish();
    }


    @Override
    public void addClusterEventListener(ClusterEventListener clusterEventListener) {
        addNewListener(clusterEventListener);
    }

    @Override
    public void removeClusterEventListener(ClusterEventListener clusterEventListener) {
        removeListener(clusterEventListener);
    }


    final void addClusterEventListenerOnTop(ClusterEventListener l)
    {
        addNewListerOnTop(l);
    }
    final void removeClusterEventListenerFromTop(ClusterEventListener l){
        removeListenerFromTop(l);
    }

    private void addNewListerOnTop(ClusterEventListener listener)
    {

        synchronized (_sync) {
            if(topList.contains(listener))
                throw new IllegalStateException("this listener registered before");

            ArrayList<ClusterEventListener> newListeners = (ArrayList) topList.clone();
            newListeners.add(listener);
            topList = newListeners;

        }
    }

    private void removeListenerFromTop(ClusterEventListener listener)
    {
        synchronized (_sync) {
            if(!topList.contains(listener))
                throw new IllegalStateException("this listener not registered");



            ArrayList<ClusterEventListener> newListeners = (ArrayList) topList.clone();
            newListeners.remove(listener);
            topList = newListeners;
        }
    }

    private void addNewListener(ClusterEventListener listener)
    {
        synchronized (_sync) {
            if( listeners.contains(listener))
                throw new IllegalStateException("this listener registered before");

            ArrayList<ClusterEventListener> newListeners = (ArrayList) listeners.clone();
            newListeners.add(listener);
            listeners = newListeners;

        }
    }

    private void removeListener(ClusterEventListener listener)
    {
        synchronized (_sync) {
            if(!listeners.contains(listener))
                throw new IllegalStateException("this listener not registered");



            ArrayList<ClusterEventListener> newListeners = (ArrayList) listeners.clone();
            newListeners.remove(listener);
            listeners = newListeners;
        }
    }

    private void handler(Object[] event)
    {
        if(event.length==1)
        {
         callListeners(cast(event[0]));
        }else if(event.length==2)
        {
            //so its a cluster state change
            callListeners(cast(event[0]) ,cast(event[1]));
        }else if (event.length==3)
        {
            callListeners(cast(event[0]) , cast(event[1]) , cast(event[2]));
        }
    }

    private final synchronized void callListeners(NodeInfo info , NodeState lastState , NodeState currentState)
    {
        for(ClusterEventListener listener:listeners)
        {
            try {
                listener.onNodeStateChanged(info, lastState, currentState);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }

    private final synchronized void callListeners(ClusterState lastState , ClusterState currentState)
    {
        for(ClusterEventListener listener:listeners)
        {
            try {
                listener.onClusterStateChanged(lastState, currentState);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }

    private final synchronized void callListeners(NodeInfo leader)
    {
        if(hasOwnThread)
        {
            logger.information("Leader Changed , new leader : [{}]" , leader.id());
        }
        for(ClusterEventListener listener:listeners)
        {
            try{
                listener.onLeaderChanged(leader);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }


    private final synchronized void callTopListeners(NodeInfo info , NodeState lastState , NodeState currentState)
    {
        if(hasOwnThread)
        {
            logger.information("Node [{}] state changed : {} --> {}" , info.id() , lastState , currentState);
        }
        for(ClusterEventListener listener:topList)
        {
            try {
                listener.onNodeStateChanged(info, lastState, currentState);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }

    private final synchronized void callTopListeners(ClusterState lastState , ClusterState currentState)
    {
        if(hasOwnThread)
        {
            logger.information("Cluster state changed : {} --> {}" , lastState , currentState);
        }
        for(ClusterEventListener listener:topList)
        {
            try {
                listener.onClusterStateChanged(lastState, currentState);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }

    private final synchronized void callTopListeners(NodeInfo leader)
    {
        for(ClusterEventListener listener:topList)
        {
            try{
                listener.onLeaderChanged(leader);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }

    private <T> T cast(Object o)
    {
        return (T)o;
    }


    void notifyClusterStateChanged(ClusterState lastState , ClusterState currentState)
    {
        callTopListeners(lastState, currentState);
        if(hasOwnThread)
            dispatcher.tryDispatch(new Object[]{lastState , currentState});
        else
            callListeners(lastState, currentState);
    }


    void notifyNodeStateChanged(NodeInfo info , NodeState lastState , NodeState currentState)
    {
        callTopListeners(info, lastState, currentState);
        if(hasOwnThread)
            dispatcher.tryDispatch(new Object[]{info , lastState , currentState});
        else
            callListeners(info, lastState, currentState);
    }

    void notifyLeaderChanged(NodeInfo leader)
    {
        callTopListeners(leader);
        if(hasOwnThread)
            dispatcher.tryDispatch(new Object[]{leader});
        else
            callListeners(leader);
    }
}
