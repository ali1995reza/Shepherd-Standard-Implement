package shepherd.utils.transport.nio.implement;

import shepherd.utils.transport.nio.model.IoContext;
import shepherd.utils.transport.nio.model.IoHandler;
import shepherd.utils.transport.nio.model.IoOperation;
import shepherd.utils.transport.nio.util.Node;
import shepherd.utils.transport.nio.util.NodeList;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

class IoThread {

    public final static class CloseThreadException extends RuntimeException {

        private CloseThreadException(){}
    }




    static class HandlerHolder
    {


        private IoHandler ioHandler;
        private SyncSelectIOState state;
        private boolean writeActive = true;
        private boolean readActive = true;
        private boolean connectActive = true;
        private boolean acceptActive = true;
        private Node nodeInConnectionList;
        private byte IDLE_STATE = 0;

        private HandlerHolder(IoHandler h , SyncSelectIOState selectState)
        {
            this.ioHandler = h;
            this.state = selectState;
        }

        private void setWriteActive()
        {
            if(writeActive)
                return;

            writeActive = true;
        }

        boolean isWriteActive()
        {
            if(state.wantToCheckIdleForOperation(IoOperation.WRITE)) {
                if (writeActive) {
                    writeActive = false;
                    return true;
                }

                IDLE_STATE |= IoOperation.WRITE.code;
                return false;
            }else {
                if(writeActive)
                    return true;

                writeActive = true;
                return true;
            }
        }

        private void setReadActive() {

            if(readActive)
                return;

            this.readActive = true;
        }

        boolean isReadActive()
        {
            if(state.wantToCheckIdleForOperation(IoOperation.READ)) {
                if (readActive) {
                    readActive = false;
                    return true;
                }
                IDLE_STATE |= IoOperation.READ.code;
                return false;
            }else {
                if(readActive)
                    return true;

                readActive = true;
                return true;
            }
        }


        boolean isConnectActive()
        {
            if(state.wantToCheckIdleForOperation(IoOperation.CONNECT)) {
                if (connectActive) {
                    connectActive = false;
                    return true;
                }
                IDLE_STATE |= IoOperation.CONNECT.code;
                return false;
            }else {
                if(connectActive)
                    return true;

                connectActive = true;
                return true;
            }
        }

        private void setConnectActive(){
            if(connectActive)
                return;

            connectActive = true;
        }

        boolean isAcceptActive()
        {
            if(state.wantToCheckIdleForOperation(IoOperation.ACCEPT)) {
                if (acceptActive) {
                    acceptActive = false;
                    return true;
                }
                IDLE_STATE |= IoOperation.ACCEPT.code;
                return false;
            }else {

                if(acceptActive)
                    return true;

                acceptActive = true;

                return true;
            }
        }

        private void setAcceptActive()
        {
            if(acceptActive)return;

            acceptActive = true;
        }

        private boolean isIdle(IoOperation operation)
        {
            if(checkOperationActiveFlag(operation))
                return false;

            return state.wantToCheckIdleForOperation(operation) &&
                    (IDLE_STATE&operation.MASK) == operation.code;
        }

        private boolean checkOperationActiveFlag(IoOperation operation)
        {
            if(operation == IoOperation.READ)
                return readActive;
            else if(operation == IoOperation.WRITE)
                return writeActive;
            else if(operation == IoOperation.ACCEPT)
                return acceptActive;
            else if(operation == IoOperation.CONNECT)
                return connectActive;
            else
                throw new IllegalArgumentException("bad io operation");
        }

        private void resetIdleState()
        {
            IDLE_STATE = 0;
        }

    }













    private Selector selector;
    private Object _sync = new Object();
    private Thread ioThread;
    private IoContext context;
    private boolean active = false;
    private boolean stopped = false;
    private CountDownLatch stopLatch = new CountDownLatch(1);
    private volatile boolean sleep = true;

    private boolean connectionToAdd;
    private ArrayDeque<SyncSelectIOState> syncCancelQueue = new ArrayDeque<>();
    private ArrayDeque<SyncSelectIOState> asyncCancelQueue = new ArrayDeque<>();
    private NodeList<HandlerHolder> connectionList = new NodeList<>();
    private IdleFinder idleFinder;

    private boolean blockingSelector = false;

    IoThread(IoContext context , ThreadFactory factory , IdleDetector detector) {
        this.context = context;
        try {
            selector = Selector.open();
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
        try {
            ioThread = factory.newThread(this::selectorLoop);
        }catch (Throwable e)
        {
            closeSelector();
            throw new IllegalStateException(e);
        }


        idleFinder = detector.registerNewIoThread(this);

    }




    boolean isInIoThread()
    {
        return Thread.currentThread() == ioThread;
    }

    public void registerIoHandler(IoHandler handler) throws IOException {

        synchronized (_sync)
        {
            checkState();
            SelectableChannel channel = handler.channel();
            connectionToAdd = true;

            SelectionKey key = null;
            try {
                wakeUpIfInBlockingMode();
                key = channel.register(selector, 0);
            }catch (Throwable e)
            {
                connectionToAdd = false;
                throw new IOException(e);
            }
            SyncSelectIOState state = new SyncSelectIOState(key , this , handler , channel.validOps());



            HandlerHolder holder = new HandlerHolder(handler , state);
            key.attach(holder);
            Node node =  connectionList.add(holder);
            holder.nodeInConnectionList = node;

            try {
                handler.onRegister(context , state);
            }catch (Throwable e)
            {
                connectionToAdd = false;
                connectionList.remove(node);
                try {
                    //ensure key canceled
                    key.cancel();
                }catch (Throwable ex){}
                throw new IOException(e);
            }



            connectionToAdd = false;
        }
    }


    public boolean isSleep() {
        return sleep;
    }


    int size()
    {
        return selector.keys().size();
    }

    void wakeup()
    {
        selector.wakeup();
    }

    void wakeUpIfSleep()
    {
        if(blockingSelector)
        {
            if(sleep)selector.wakeup();
        }
    }
    void wakeUpIfInBlockingMode()
    {
        if(blockingSelector) selector.wakeup();
    }

    private void selectorLoop()
    {
        while (true) {
            try {

                Iterator<SelectionKey> keys = select();

                while (keys.hasNext())
                {
                    SelectionKey key = keys.next();
                    keys.remove();
                    HandlerHolder holder = (HandlerHolder)key.attachment();


                    if(holder.state.wantToCancel())
                    {
                        continue;
                    }


                    if(key.isValid() && key.isReadable())
                    {
                        holder.setReadActive();
                        callRead(holder.ioHandler);
                        if(holder.state.wantToCancel()) {
                            continue;
                        }
                    }

                    if(key.isValid() && key.isWritable())
                    {
                        holder.setWriteActive();
                        callWrite(holder.ioHandler);
                        if(holder.state.wantToCancel()) {
                            continue;
                        }
                    }

                    if(key.isValid() && key.isAcceptable())
                    {
                        holder.setAcceptActive();
                        callAccept(holder.ioHandler);
                        if(holder.state.wantToCancel()) {
                            continue;
                        }
                    }

                    if(key.isValid() && key.isConnectable())
                    {
                        holder.setConnectActive();
                        callConnect(holder.ioHandler);
                    }


                }

                handleSynchronousOperations();


            }catch (CloseThreadException closeException){
                closeSelector();
                stopLatch.countDown();
                return;
            }catch (Throwable e)
            {
                e.printStackTrace();
                return;
                //todo handle exception please !
            }
        }
    }


    private final Iterator<SelectionKey> select() throws CloseThreadException, IOException {
        //so handle it please !v
        int numberOfSelectedChannels = -1;
        while (active)
        {
            if(blockingSelector)
            {

                sleep = true;
                numberOfSelectedChannels = selector.select();
                sleep = false;

                if(numberOfSelectedChannels==0)
                {

                    handleSynchronousOperations();
                }else
                {
                    return selector.selectedKeys().iterator();
                }
            }else
            {
                numberOfSelectedChannels = selector.selectNow();

                if(numberOfSelectedChannels==0) {

                    //sleep(1);

                    numberOfSelectedChannels = selector.select(1);
                    if(numberOfSelectedChannels==0) {

                        handleSynchronousOperations();
                        continue;
                    }else
                    {
                        return selector.selectedKeys().iterator();
                    }
                }else
                {
                    return selector.selectedKeys().iterator();
                }
            }
        }

        throw new CloseThreadException();
    }



    private void throwCloseThreadExceptionIfNeed() throws CloseThreadException {
        if(active)return;

        throw new CloseThreadException();
    }


    private void handleSynchronousOperations()
    {

        handleIdles();
        handleAsyncCancelQueue();
        handleSyncCancelQueue();
        waitForConnectionToAdd();
    }


    void start()
    {
        synchronized (_sync)
        {
            if(stopped)
                throw new IllegalStateException("IoThread already stopped");

            if(active)
                throw new IllegalStateException("IoThread already started");


            try {
                active = true;
                ioThread.start();

            }catch (Throwable e)
            {
                closeSelector();
                stopped = true;
                throw e;
            }
        }
    }

    private final void handleSyncCancelQueue()
    {
        if(syncCancelQueue.size()>0)
        {
            SyncSelectIOState state = syncCancelQueue.pollFirst();
            while (state!=null)
            {
                handleCancel((HandlerHolder)state.selectionKey().attachment());
                state = syncCancelQueue.pollFirst();
            }
        }

    }


    private final void handleAsyncCancelQueue()
    {
        if(asyncCancelQueue.size()>0) {
            synchronized (asyncCancelQueue) {

                SyncSelectIOState state = asyncCancelQueue.pollFirst();
                while (state!=null)
                {
                    handleCancel((HandlerHolder)state.selectionKey().attachment());
                    state = asyncCancelQueue.pollFirst();
                }


            }
        }
    }

    private final void waitForConnectionToAdd()
    {
        if(connectionToAdd)
        {
            synchronized (_sync)
            {

            }
        }
    }

    void stop(final List<IoHandler> ioHandlers)
    {
        synchronized (_sync) {
            if (stopped)
                throw new IllegalStateException("IoThread already stopped");

            stopped = true;
            if(!active)
                return;

            active = false;

            wakeUpIfInBlockingMode();
        }


        if(Thread.currentThread()!=ioThread)
        {
            try {
                stopLatch.await();
            } catch (InterruptedException e) {
                while (stopLatch.getCount() > 0) ;
            }
        }



        if(ioHandlers!=null) {
            connectionList.forEach(new Consumer<HandlerHolder>() {
                @Override
                public void accept(HandlerHolder handlerHolder) {
                    ioHandlers.add(handlerHolder.ioHandler);
                }
            });
        }
    }

    private final void closeSelector()
    {
        try {
            selector.close();
        } catch (IOException e) {
        }
    }


    void stop()
    {
        stop(null);
    }

    final NodeList<HandlerHolder> connectionList()
    {
        return connectionList;
    }

    void putToSyncCancelQueue(SyncSelectIOState state)
    {
        syncCancelQueue.add(state);
    }

    void putToAsyncCancelQueue(SyncSelectIOState state)
    {
        synchronized (asyncCancelQueue) {
            asyncCancelQueue.add(state);
        }
    }

    private void checkState()
    {

        if(stopped)
            throw new IllegalStateException("IoThread already stopped");

        if(!active)
            throw new IllegalStateException("IoThread not start yet");
    }


    private void callRead(IoHandler handler)
    {
        try{
            handler.read(context);
        }catch (Throwable e)
        {
            callExceptionHandler(e , handler , IoOperation.READ);
        }

        throwCloseThreadExceptionIfNeed();
    }


    private void callWrite(IoHandler handler)
    {
        try{
            handler.write(context);
        }catch (Throwable e)
        {
            callExceptionHandler(e , handler , IoOperation.WRITE);
        }

        throwCloseThreadExceptionIfNeed();
    }


    private void callAccept(IoHandler handler)
    {
        try{
            handler.accept(context);
        }catch (Throwable e)
        {
            callExceptionHandler(e , handler , IoOperation.ACCEPT);
        }

        throwCloseThreadExceptionIfNeed();
    }

    private void callConnect(IoHandler handler){

        try{

            handler.connect(context);
        }catch (Throwable e)
        {
            callExceptionHandler(e , handler , IoOperation.CONNECT);
        }
    }


    private void handleCancel(HandlerHolder handlerHolder)
    {
        try{
            handlerHolder.state.selectionKey().cancel();
        }catch (Throwable e)
        {

        }

        connectionList.remove(handlerHolder.nodeInConnectionList);


        //dont do it more please !
        //ensureCanceledKeyRemovedFromSelectorKeys();

        handlerHolder.state.notifyCancelDone();



        callCancel(handlerHolder.ioHandler);

        throwCloseThreadExceptionIfNeed();
    }

    private void callCancel(IoHandler ioHandler)
    {

        try{
            ioHandler.onCanceled(context);
        }catch (Throwable e)
        {
            callExceptionHandler(e , ioHandler , IoOperation.CANCEL);
        }

        throwCloseThreadExceptionIfNeed();
    }

    private void callIdle(IoHandler ioHandler , IoOperation idleOp)
    {
        try{
            ioHandler.onIdle(context , idleOp);
        }catch (Throwable e)
        {
            callExceptionHandler(e , ioHandler , IoOperation.IDLE);
        }

        throwCloseThreadExceptionIfNeed();
    }


    private void callExceptionHandler(Throwable e , IoHandler handler , IoOperation operation)
    {
        try{

            handler.onException(e  , operation);
        }catch (Throwable ex)
        {
            ex.printStackTrace();
        }
    }



    private final void ensureCanceledKeyRemovedFromSelectorKeys() throws IOException {
        int i = selector.selectNow();
        if(i>0)
        {
            selector.selectedKeys().clear();
        }
    }


    private final void forEachIdles(HandlerHolder holder)
    {
        if(holder.state.wantToCancel()) {
            holder.resetIdleState();
            return;
        }


        if(holder.isIdle(IoOperation.READ))
        {
            callIdle(holder.ioHandler , IoOperation.READ);
            holder.setReadActive();
        }

        if(holder.state.wantToCancel()) {
            holder.resetIdleState();
            return;
        }

        if(holder.isIdle(IoOperation.WRITE))
        {
            callIdle(holder.ioHandler , IoOperation.WRITE);
            holder.setWriteActive();
        }

        if(holder.state.wantToCancel()) {
            holder.resetIdleState();
            return;
        }

        if(holder.isIdle(IoOperation.ACCEPT))
        {
            callIdle(holder.ioHandler , IoOperation.ACCEPT);
            holder.setAcceptActive();
        }

        if(holder.state.wantToCancel()) {
            holder.resetIdleState();
            return;
        }

        if(holder.isIdle(IoOperation.CONNECT))
        {
            callIdle(holder.ioHandler , IoOperation.CONNECT);
            holder.setConnectActive();
        }

        holder.resetIdleState();

    }

    private final void handleIdles( )
    {
        if(idleFinder.hasIdleToCheck())
        {
            //if(logActive) System.out.println("ACTIVE");
            //else System.out.println("NOT_ACTIVE");
            idleFinder.forEach(this::forEachIdles);
        }
    }


    private final void assertIfStopped()
    {
        if(stopped)
        {
            throw new IllegalStateException("io thread already stopped");
        }
    }


    final void setBlockingSelector(boolean s)
    {
        synchronized (_sync)
        {
            assertIfStopped();
            if(blockingSelector==s)return;
            blockingSelector = s;
            if(blockingSelector && active)
            {
                //so must call a wakeup
                wakeup();
            }
        }
    }



}
