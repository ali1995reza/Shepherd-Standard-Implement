package shepherd.standard.cluster.node;

import shepherd.api.cluster.node.NodeInfo;
import shepherd.api.message.Response;

class ResponseImpl<T> extends MessageImpl<T> implements Response<T> {


    private final Error error;

    ResponseImpl(T d, int i, NodeInfo s, byte t, long sTime, long rTime , Error err) {
        super(d, i, s, t, sTime, rTime);
        error = err;
    }

    @Override
    public boolean hasError() {
        return error != null;
    }

    @Override
    public Error error() {
        return error;
    }


}
