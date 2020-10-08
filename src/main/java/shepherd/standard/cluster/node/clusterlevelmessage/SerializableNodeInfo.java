package shepherd.standard.cluster.node.clusterlevelmessage;

import java.net.SocketAddress;
import java.util.Objects;

public class SerializableNodeInfo implements ClusterMessage {

    private int id;
    private SocketAddress address;
    private long joinTime;
    private long version;
    private boolean leader;
    private String hashId;


    public SerializableNodeInfo setId(int id) {
        this.id = id;
        return this;
    }

    public SerializableNodeInfo setAddress(SocketAddress address) {
        this.address = address;
        return this;
    }

    public SerializableNodeInfo setJoinTime(long joinTime) {
        this.joinTime = joinTime;
        return this;
    }

    public SerializableNodeInfo setVersion(long version) {
        this.version = version;
        return this;
    }

    public SerializableNodeInfo setLeader(boolean leader) {
        this.leader = leader;
        return this;
    }

    public SerializableNodeInfo setHashId(String hashId) {
        this.hashId = hashId;
        return this;
    }

    public String hashId() {
        return hashId;
    }

    public SocketAddress address() {
        return address;
    }

    public int id() {
        return id;
    }



    public long version() {
        return version;
    }

    public long joinTime() {
        return joinTime;
    }

    public boolean isLeader() {
        return leader;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SerializableNodeInfo info = (SerializableNodeInfo) o;
        return id == info.id &&
                joinTime == info.joinTime &&
                version == info.version &&
                leader == info.leader &&
                Objects.equals(address, info.address) &&
                Objects.equals(hashId, info.hashId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, address, joinTime, version, leader, hashId);
    }


    @Override
    public String toString() {
        return "SerializableNodeInfo{" +
                "id=" + id +
                ", address=" + address +
                ", joinTime=" + joinTime +
                ", version=" + version +
                ", leader=" + leader +
                ", hashId='" + hashId + '\'' +
                '}';
    }
}
