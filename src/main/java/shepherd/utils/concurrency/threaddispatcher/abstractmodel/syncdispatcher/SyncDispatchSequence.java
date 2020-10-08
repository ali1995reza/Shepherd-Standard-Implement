package shepherd.utils.concurrency.threaddispatcher.abstractmodel.syncdispatcher;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CountDownLatch;

final class SyncDispatchSequence<I , T , QT> {

    private final Deque<QT> queue;
    private boolean enqueued = false;
    final I syncId;
    private final DispatchQueueMethods<I,T,QT> dispatcher;
    private CountDownLatch stopLatch;
    private final Object _sync = new Object();
    private Thread controllerThread;
    private boolean closed = false;

    SyncDispatchSequence(DispatchQueueMethods<I,T,QT> dispatcher, I sId)
    {
        queue = new ArrayDeque<>(1000);
        this.dispatcher = dispatcher;
        syncId = sId;
    }



    void close(CountDownLatch stopLatch)
    {
        synchronized (_sync) {
            if(closed)
                throw new IllegalStateException("sequence closed before");

            this.stopLatch = stopLatch;
            closed = true;
            if (!enqueued) {
                if (queue.isEmpty()) {
                    //so it must called imm
                    stopLatch.countDown();
                }
            }
        }
    }

    void close()
    {
        synchronized (_sync) {
            if(closed)
                throw new IllegalStateException("sequence closed before");

            closed = true;
        }
    }

    boolean putNext(T data)
    {


        synchronized (_sync) {

            if(closed)return false;


            if (!dispatcher.putInSyncQueue(syncId, data, queue)) return false;

            if (enqueued) return true;

            enqueued = true;
        }
        dispatcher.addToHandlerQueue(this);

        return true;
    }


    QT getNext()
    {
        controllerThread = Thread.currentThread();

        synchronized (_sync) {
            return queue.pollFirst();
        }
    }

    void enqueueForNext()
    {
        synchronized (_sync) {
            if (queue.isEmpty()) {
                enqueued = false;
                if (stopLatch != null)
                    stopLatch.countDown();


                controllerThread = null;

                return;
            }
        }

        dispatcher.addToHandlerQueue(this);

        controllerThread = null;
    }


    final Deque<QT> getQueue()
    {
        return queue;
    }

    final Thread controllerThread()
    {
        return controllerThread;
    }



}
