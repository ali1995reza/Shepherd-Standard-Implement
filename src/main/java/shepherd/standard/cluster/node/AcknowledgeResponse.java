package shepherd.standard.cluster.node;

class AcknowledgeResponse {

    private final int ackId;
    private final StandardNodeInfo node;
    private final int code;
    private final byte priority;

    AcknowledgeResponse(int ack , int c , StandardNodeInfo info , byte priority)
    {
        ackId = ack;
        node = info;
        this.code = c;
        this.priority = priority;
    }


    AcknowledgeResponse(int ack, StandardNodeInfo info , byte priority)
    {
        this(ack , 1 , info , priority);
    }

    void  handle()
    {
        node.notifyAcknowledges(priority , ackId , code);
    }

    public int acknowledgeId() {
        return ackId;
    }

    public StandardNodeInfo node() {
        return node;
    }

    public byte priority() {
        return priority;
    }
}
