package shepherd.standard.cluster.node.clusterlevelmessage;

public class No implements ClusterMessage {


    public final static No NO = new No();


    private final static String NO_STR = "No";

    @Override
    public String toString() {
        return NO_STR;
    }
}
