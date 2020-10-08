package shepherd.utils.transport.nio.implement;

import shepherd.utils.transport.nio.util.NodeList;

import java.util.ArrayDeque;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

class IdleFinder {

    private NodeList<IoThread.HandlerHolder> ioHandlers;
    private ArrayDeque<IoThread.HandlerHolder> deque;
    private IoThread ioThread;
    private CountDownLatch forEachDoneLatch ;
    private boolean foundLatch;
    private boolean hasIdleToCheck;

    public IdleFinder(IoThread t)
    {
        deque = new ArrayDeque<>();
        ioThread = t;
        ioHandlers = ioThread.connectionList();
    }


    boolean check()
    {
        //will change this method please !

        ioHandlers.forEach(this::checkLoop);
        foundLatch = deque.size()>0;
        return foundLatch;
    }

    boolean setLatchIfIdlesFound(CountDownLatch latch)
    {
        if(foundLatch)
        {
            forEachDoneLatch = latch;
            hasIdleToCheck = true;
            foundLatch = false;

            return true;
        }

        return false;
    }



    private void checkLoop(IoThread.HandlerHolder holder)
    {
        boolean writeIdle = !holder.isWriteActive();
        boolean readIdle = !holder.isReadActive();
        boolean acceptIdle = !holder.isAcceptActive();
        boolean connectIdle = !holder.isConnectActive();
        if(writeIdle || readIdle || acceptIdle || connectIdle) {
            deque.add(holder);
        }
    }

    boolean hasIdleToCheck()
    {
        return hasIdleToCheck;
    }


    void forEach(Consumer<IoThread.HandlerHolder> action)
    {
        if(!hasIdleToCheck)
            throw new IllegalStateException("no idle to check");

        IoThread.HandlerHolder holder = deque.pollFirst();

        while (holder!=null)
        {
            try {
                action.accept(holder);
            }catch (IoThread.CloseThreadException e){

                throw e;
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
            holder = deque.pollFirst();
        }

        hasIdleToCheck = false;

        forEachDoneLatch.countDown();
    }


    public IoThread ioThread() {
        return ioThread;
    }


}
