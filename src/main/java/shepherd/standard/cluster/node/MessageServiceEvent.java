package shepherd.standard.cluster.node;

interface MessageServiceEvent {


    int serviceId();

    void handleBy(MessageServiceImpl service);
}
