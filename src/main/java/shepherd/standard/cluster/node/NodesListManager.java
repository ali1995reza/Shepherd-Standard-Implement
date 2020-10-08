package shepherd.standard.cluster.node;

import shepherd.api.cluster.ClusterSchema;
import shepherd.api.cluster.node.NodeInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

class NodesListManager implements ClusterSchema {


    private HashMap<Integer , NodeInfoImpl> immutableMap;
    private ConcurrentHashMap<Integer , NodeInfo> mutableMap;
    private Map<Integer , NodeInfo> unModifiableMap;
    private NodeInfoImpl[] immutableList;
    private List<NodeInfoImpl> mutableList;
    private NodeInfoImpl[] listMap;

    private final Object _sync = new Object();
    private int size = 0;

    private NodeInfoImpl leader;

    NodesListManager()
    {
        immutableMap = new HashMap<>();
        mutableMap = new ConcurrentHashMap<>();
        unModifiableMap = Collections.unmodifiableMap(mutableMap);
        immutableList = new NodeInfoImpl[0];
        listMap = new NodeInfoImpl[0];
        mutableList = new ArrayList<>();
    }


    NodeInfoImpl setNextLeader()
    {
        synchronized (_sync)
        {
            NodeInfoImpl nextLeader = immutableList[0];

            for(NodeInfoImpl info:immutableList)
            {
                if(info == nextLeader)continue;

                if(nextLeader.joinTime()>info.joinTime())
                {
                    nextLeader = info;
                }
            }

            nextLeader.setAsLeader();
            leader = nextLeader;
            return nextLeader;
        }
    }


    @Override
    public Map<Integer, NodeInfo> nodes() {
        return unModifiableMap;
    }

    @Override
    public NodeInfo leader() {
        return leader;
    }


    @Override
    public int[] filterNodes(Function<NodeInfo, Boolean> filter) {
        List<NodeInfo> arrayList = new ArrayList<>();
        NodeInfoImpl[] list = immutableList;

        for(NodeInfoImpl nodeInfo : list)
        {
            if(filter.apply(nodeInfo))
                arrayList.add(nodeInfo);
        }

        int[] nodes = new int[arrayList.size()];
        for(int i=0;i<nodes.length;i++)
        {
            nodes[i] = arrayList.get(i).id();
        }

        return nodes;
    }

    NodeInfoImpl leaderInfoImpl()
    {
        return leader;
    }

    void addNode(NodeInfoImpl info)
    {
        synchronized (_sync)
        {
            if(immutableMap.containsKey(info.id()))
                throw new IllegalStateException("this list already has a node with this id");

            if(info.id()<0)
                throw new IllegalStateException("node id cant less than 0");

            HashMap<Integer , NodeInfoImpl> newMap = copyImmutableMap();
            newMap.put(info.id() , info);
            mutableMap.put(info.id() , info);
            immutableMap = newMap;

            addToList(info);
            addToListMap(info);
            ++size;
        }
    }


    void removeNode(NodeInfoImpl info)
    {
        synchronized (_sync)
        {
            if(!immutableMap.containsValue(info))
                throw new IllegalStateException("this immutableList dose not contains this node");

            HashMap<Integer , NodeInfoImpl> newMap = copyImmutableMap();
            newMap.remove(info.id() , info);
            mutableMap.remove(info.id() , info);
            immutableMap = newMap;

            removeFromList(info);
            removeFromListMap(info);

            --size;
        }
    }

    void removeNode(Integer id)
    {
        synchronized (_sync)
        {
            if(!immutableMap.containsKey(id))
                throw new IllegalStateException("this immutableList dose not contains node with this id");

            HashMap<Integer , NodeInfoImpl> newMap = copyImmutableMap();
            newMap.remove(id);
            mutableMap.remove(id);
            immutableMap = newMap;

            removeFromList(id);
            removeFromListMap(id);

            --size;
        }
    }



    Map<Integer , NodeInfoImpl> immutableMap()
    {
        return immutableMap;
    }

    Map<Integer , NodeInfo> mutableMap()
    {
        return mutableMap;
    }

    Map<Integer , NodeInfo> unmodifiableMutableMap()
    {
        return unModifiableMap;
    }

    NodeInfoImpl[] immutableList()
    {
        return immutableList;
    }

    List<NodeInfoImpl> mutableList()
    {
        return mutableList;
    }

    NodeInfoImpl fastFindById(final int id)
    {
        if(id<0 || listMap.length<=id)
            return null;

        return listMap[id];
    }

    NodeInfoImpl findById(int id)
    {
        return immutableMap.get(id);
    }

    int size()
    {
        return size;
    }

    private void removeFromList(final NodeInfoImpl info)
    {
        if(!mutableList.removeIf(new Predicate<NodeInfoImpl>() {
            @Override
            public boolean test(NodeInfoImpl i) {
                return i==info;
            }
        })){

            throw new IllegalStateException("this node is not in list");
        }

        NodeInfoImpl[] newList = new NodeInfoImpl[immutableList.length-1];
        for(int i=0;i<newList.length;i++)
        {
            newList[i] = mutableList.get(i);
        }

        immutableList = newList;
    }


    private void removeFromList(final int id)
    {
        if(!mutableList.removeIf(new Predicate<NodeInfoImpl>() {
            @Override
            public boolean test(NodeInfoImpl info) {
                return info.id()==id;
            }
        })){

            throw new IllegalStateException("there is no node with this id in list");
        }
        NodeInfoImpl[] newList = new NodeInfoImpl[immutableList.length-1];

        for(int i=0;i<newList.length;i++)
        {
            newList[i] = mutableList.get(i);
        }

        immutableList = newList;
    }


    private void addToList(final NodeInfoImpl info)
    {
        mutableList.add(info);
        NodeInfoImpl[] newList = new NodeInfoImpl[immutableList.length+1];
        for(int i=0;i<newList.length;i++)
        {
            newList[i] = mutableList.get(i);
        }

        immutableList = newList;
    }

    private void removeFromListMap(final NodeInfoImpl info)
    {
        if(fastFindById(info.id())!=null && fastFindById(info.id()) == info) {
            listMap[info.id()] = null;
        }
    }

    private void removeFromListMap(final int id)
    {
        if(fastFindById(id)!=null && fastFindById(id).id() == id) {
            listMap[id] = null;
        }
    }

    private void addToListMap(final NodeInfoImpl info)
    {
        if(info.id()>listMap.length-1)
        {
            NodeInfoImpl[] newList = new NodeInfoImpl[info.id()+1];
            for(int i=0;i<listMap.length;i++)
            {
                newList[i] = listMap[i];
            }

            newList[info.id()] = info;
            listMap = newList;
        }else
        {
            listMap[info.id()] = info;
        }
    }

    private HashMap<Integer , NodeInfoImpl> copyImmutableMap()
    {

        return (HashMap) immutableMap.clone();
    }

}
