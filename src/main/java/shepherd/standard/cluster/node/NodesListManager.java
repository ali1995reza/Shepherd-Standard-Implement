package shepherd.standard.cluster.node;

import shepherd.api.cluster.ClusterSchema;
import shepherd.api.cluster.node.NodeInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

class NodesListManager implements ClusterSchema {


    private HashMap<Integer , StandardNodeInfo> immutableMap;
    private ConcurrentHashMap<Integer , NodeInfo> mutableMap;
    private Map<Integer , NodeInfo> unModifiableMap;
    private StandardNodeInfo[] immutableList;
    private List<StandardNodeInfo> mutableList;
    private StandardNodeInfo[] listMap;

    private final Object _sync = new Object();
    private int size = 0;

    private StandardNodeInfo leader;

    NodesListManager()
    {
        immutableMap = new HashMap<>();
        mutableMap = new ConcurrentHashMap<>();
        unModifiableMap = Collections.unmodifiableMap(mutableMap);
        immutableList = new StandardNodeInfo[0];
        listMap = new StandardNodeInfo[0];
        mutableList = new ArrayList<>();
    }


    StandardNodeInfo setNextLeader()
    {
        synchronized (_sync)
        {
            StandardNodeInfo nextLeader = immutableList[0];

            for(StandardNodeInfo info:immutableList)
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
        StandardNodeInfo[] list = immutableList;

        for(StandardNodeInfo nodeInfo : list)
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

    StandardNodeInfo leaderInfoImpl()
    {
        return leader;
    }

    void addNode(StandardNodeInfo info)
    {
        synchronized (_sync)
        {
            if(immutableMap.containsKey(info.id()))
                throw new IllegalStateException("this list already has a node with this id");

            if(info.id()<0)
                throw new IllegalStateException("node id cant less than 0");

            HashMap<Integer , StandardNodeInfo> newMap = copyImmutableMap();
            newMap.put(info.id() , info);
            mutableMap.put(info.id() , info);
            immutableMap = newMap;

            addToList(info);
            addToListMap(info);
            ++size;
        }
    }


    void removeNode(StandardNodeInfo info)
    {
        synchronized (_sync)
        {
            if(!immutableMap.containsValue(info))
                throw new IllegalStateException("this immutableList dose not contains this node");

            HashMap<Integer , StandardNodeInfo> newMap = copyImmutableMap();
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

            HashMap<Integer , StandardNodeInfo> newMap = copyImmutableMap();
            newMap.remove(id);
            mutableMap.remove(id);
            immutableMap = newMap;

            removeFromList(id);
            removeFromListMap(id);

            --size;
        }
    }



    Map<Integer , StandardNodeInfo> immutableMap()
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

    StandardNodeInfo[] immutableList()
    {
        return immutableList;
    }

    List<StandardNodeInfo> mutableList()
    {
        return mutableList;
    }

    StandardNodeInfo fastFindById(final int id)
    {
        if(id<0 || listMap.length<=id)
            return null;

        return listMap[id];
    }

    StandardNodeInfo findById(int id)
    {
        return immutableMap.get(id);
    }

    int size()
    {
        return size;
    }

    private void removeFromList(final StandardNodeInfo info)
    {
        if(!mutableList.removeIf(new Predicate<StandardNodeInfo>() {
            @Override
            public boolean test(StandardNodeInfo i) {
                return i==info;
            }
        })){

            throw new IllegalStateException("this node is not in list");
        }

        StandardNodeInfo[] newList = new StandardNodeInfo[immutableList.length-1];
        for(int i=0;i<newList.length;i++)
        {
            newList[i] = mutableList.get(i);
        }

        immutableList = newList;
    }


    private void removeFromList(final int id)
    {
        if(!mutableList.removeIf(new Predicate<StandardNodeInfo>() {
            @Override
            public boolean test(StandardNodeInfo info) {
                return info.id()==id;
            }
        })){

            throw new IllegalStateException("there is no node with this id in list");
        }
        StandardNodeInfo[] newList = new StandardNodeInfo[immutableList.length-1];

        for(int i=0;i<newList.length;i++)
        {
            newList[i] = mutableList.get(i);
        }

        immutableList = newList;
    }


    private void addToList(final StandardNodeInfo info)
    {
        mutableList.add(info);
        StandardNodeInfo[] newList = new StandardNodeInfo[immutableList.length+1];
        for(int i=0;i<newList.length;i++)
        {
            newList[i] = mutableList.get(i);
        }

        immutableList = newList;
    }

    private void removeFromListMap(final StandardNodeInfo info)
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

    private void addToListMap(final StandardNodeInfo info)
    {
        if(info.id()>listMap.length-1)
        {
            StandardNodeInfo[] newList = new StandardNodeInfo[info.id()+1];
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

    private HashMap<Integer , StandardNodeInfo> copyImmutableMap()
    {

        return (HashMap) immutableMap.clone();
    }

    void clear()
    {
        immutableMap.clear();
        mutableMap.clear();
        clearList(immutableList);
        mutableList.clear();
        clearList(listMap);

        immutableMap = null;
        mutableMap = null;
        unModifiableMap = null;
        immutableList = null;
        mutableList = null;
        listMap = null;
    }

    private final static <T> void clearList(T[] obj)
    {
        for(int i=0;i<obj.length;i++)
        {
            obj[i] = null;
        }
    }
}
