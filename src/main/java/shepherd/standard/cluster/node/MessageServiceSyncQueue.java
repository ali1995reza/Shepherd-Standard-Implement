package shepherd.standard.cluster.node;

interface MessageServiceSyncQueue {

    void enqueue(MessageServiceEvent event);

}
