package shepherd.standard.cluster.node.clusterlevelmessage;


import shepherd.standard.cluster.node.JoinHashCalculator;

public class ReadyAnnounce implements Announce{

    public final static ReadyAnnounce withHashId(String hash)
    {
        return new ReadyAnnounce(hash);
    }

    public final static ReadyAnnounce withHashId(JoinHashCalculator calculator)
    {
        return withHashId(calculator.calculateHash());
    }


    private String joinHash;

    public ReadyAnnounce(String  joinHash){
        this.joinHash = joinHash;
    }

    public ReadyAnnounce setJoinHash(String joinHash) {
        this.joinHash = joinHash;
        return this;
    }

    public String joinHash() {
        return joinHash;
    }
}
