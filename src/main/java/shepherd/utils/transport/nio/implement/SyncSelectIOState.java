package shepherd.utils.transport.nio.implement;


import shepherd.utils.transport.nio.model.IoHandler;
import shepherd.utils.transport.nio.model.IoOperation;
import shepherd.utils.transport.nio.model.IoState;

import java.nio.channels.SelectionKey;
import java.util.concurrent.CountDownLatch;

final class SyncSelectIOState implements IoState
{
    private final static int READ = SelectionKey.OP_READ;
    private final static int WRITE = SelectionKey.OP_WRITE;
    private final static int CONNECT = SelectionKey.OP_CONNECT;
    private final static int ACCEPT = SelectionKey.OP_ACCEPT;
    private final static int READ_WRITE = SelectionKey.OP_READ|SelectionKey.OP_WRITE;
    private final static int NOTHING = 0;


    private SelectionKey key;
    private IoThread ioThread;
    private boolean canceled = false;
    private IoHandler handler;
    private final CountDownLatch cancelLatch = new CountDownLatch(1);
    private byte idleOperationsCheck = NOTHING;
    private final int validOps;


    SyncSelectIOState(SelectionKey key , IoThread t , IoHandler handler , int validOps)
    {
        this.key = key;
        this.ioThread = t;
        this.handler = handler;
        this.validOps = validOps;
    }


    @Override
    public void doRead() {

        if(canceled)
            throw new IllegalStateException("io state canceled");



        if(key.interestOps()!=READ) {
            key.interestOps(READ);
            ioThread.wakeUpIfSleep();
        }
    }

    @Override
    public void doWrite() {

        if(canceled)
            throw new IllegalStateException("io state canceled");


        if(key.interestOps()!=WRITE) {
            key.interestOps(WRITE);
            ioThread.wakeUpIfSleep();
        }
    }

    @Override
    public void doAccept() {
        if(canceled)
            throw new IllegalStateException("io state canceled");


        if(key.interestOps()!=ACCEPT) {
            key.interestOps(ACCEPT);
            ioThread.wakeUpIfSleep();
        }
    }

    @Override
    public void doConnect() {
        if(canceled)
            throw new IllegalStateException("io state canceled");


        if(key.interestOps()!=CONNECT) {
            key.interestOps(CONNECT);
            ioThread.wakeUpIfSleep();
        }
    }

    @Override
    public void doNothing() {


        if(canceled)
            throw new IllegalStateException("io state canceled");



        if(key.interestOps()!=NOTHING) {
            key.interestOps(NOTHING);
            ioThread.wakeUpIfSleep();
        }
    }


    @Override
    public void doReadAndWrite() {


        if(canceled)
            throw new IllegalStateException("io state canceled");


        if(key.interestOps()!=READ_WRITE) {
            key.interestOps(READ_WRITE);
            ioThread.wakeUpIfSleep();
        }
    }

    @Override
    public void doOperations(IoOperation... ops) {
        if(canceled)
            throw new IllegalStateException("io state canceled");


        int opsValue = 0;

        for(IoOperation op:ops) {
            ensureValidIoOperation(op);
            opsValue|=op.code;
        }

        if(key.interestOps()!=opsValue) {
            key.interestOps(opsValue);
            ioThread.wakeUpIfSleep();
        }
    }

    @Override
    public void doOperation(IoOperation op) {

        if(canceled)
            throw new IllegalStateException("io state canceled");

        ensureValidIoOperation(op);

        if(key.interestOps()!=op.code) {
            key.interestOps(op.code);
            ioThread.wakeUpIfSleep();
        }

    }

    @Override
    public void cancel() {

        synchronized (cancelLatch) {
            if(!canceled) {
                if(isInIoThread())
                {
                    //put to syncQueue
                    ioThread.putToSyncCancelQueue(this);
                }else
                {
                    //put to asyncQueue
                    ioThread.putToAsyncCancelQueue(this);
                    ioThread.wakeup();
                }
                canceled = true;
            }
        }

        if(!isInIoThread())
        {
            try {
                cancelLatch.await();
            } catch (InterruptedException e) {
                while (cancelLatch.getCount()>0);
            }
        }
    }


    @Override
    public boolean isInIoThread() {
        return ioThread.isInIoThread();
    }

    @Override
    public void setOperationsToCheckIdle(IoOperation ... operations) {
        if(operations==null)
            idleOperationsCheck = NOTHING;

        if(operations.length==0)
            idleOperationsCheck = NOTHING;

        byte idlOps = 0;

        for(IoOperation operation:operations)
        {
            ensureValidIoOperation(operation);
            idlOps|=operation.code;
        }

        idleOperationsCheck = idlOps;
    }

    @Override
    public boolean containsOperation(IoOperation ioOperation) {

        return (selectionKey().interestOps() & ioOperation.code) != 0 ;

    }

    @Override
    public boolean containsOperations(IoOperation... ioOperations) {

        int ops = selectionKey().interestOps();


        for(IoOperation ioOperation : ioOperations){
            if((ops & ioOperation.code) == 0 )
                return false;
        }


        return true;
    }

    @Override
    public boolean isCanceled() {
        return canceled;
    }

    @Override
    public IoHandler ioHandler() {
        return handler;
    }

    final boolean wantToCancel()
    {
        return canceled;
    }

    final boolean wantToCheckIdleForOperation(IoOperation operation)
    {
        return (operation.MASK & idleOperationsCheck) == operation.code;
    }

    SelectionKey selectionKey()
    {
        return key;
    }




    final void notifyCancelDone()
    {
        synchronized (cancelLatch)
        {
            if(!canceled)
                throw new IllegalStateException("cancel not requested");
        }
        cancelLatch.countDown();
    }

    private void ensureValidIoOperation(IoOperation ioOperation)
    {
        if((ioOperation.MASK & validOps) != ioOperation.code)
            throw new IllegalArgumentException(ioOperation+" is not a valid io operation");
    }

}
