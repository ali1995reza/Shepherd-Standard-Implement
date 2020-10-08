package shepherd.standard.cluster.node;

import shepherd.api.config.ConfigurationKey;

import java.net.InetSocketAddress;

public class NodeConfigurations {


    public final static ConfigurationKey<InetSocketAddress> NODE_ADDRESS =
            new ConfigurationKey<>("node.address");
    public final static ConfigurationKey<Integer> NUMBER_OF_MESSAGE_EVENT_HANDLER_THREADS =
            new ConfigurationKey<>("number.of.message.event.handler.threads") ;
    public final static ConfigurationKey<Integer> ACKNOWLEDGE_SEND_INTERVAL =
            new ConfigurationKey<>("acknowledge.send.interval");
    public final static ConfigurationKey<Integer> NUMBER_OF_ACKNOWLEDGE_HANDLER_THREADS =
            new ConfigurationKey<>("number.of.acknowledge.handler.threads");
    public final static ConfigurationKey<String> JOIN_PASSWORD =
            new ConfigurationKey<>("node.join.password");
}
