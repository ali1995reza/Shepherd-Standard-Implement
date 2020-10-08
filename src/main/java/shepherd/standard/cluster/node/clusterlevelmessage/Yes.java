package shepherd.standard.cluster.node.clusterlevelmessage;

public class Yes implements ClusterMessage {

    public final static Yes YES = new Yes();



    private final static String YES_STR = "Yes";

    @Override
    public String toString() {
        return YES_STR;
    }
}
