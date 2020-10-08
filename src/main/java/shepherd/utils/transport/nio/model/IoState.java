package shepherd.utils.transport.nio.model;

public interface IoState {

    void doRead();
    void doWrite();
    void doAccept();
    void doConnect();
    void doReadAndWrite();


    void doOperations(IoOperation... ops);
    void doOperation(IoOperation op);

    void doNothing();
    void cancel();
    boolean isInIoThread();
    void setOperationsToCheckIdle(IoOperation... operations);


    boolean containsOperation(IoOperation ioOperation);
    boolean containsOperations(IoOperation... ioOperations);
    boolean isCanceled();

    IoHandler ioHandler();
}
