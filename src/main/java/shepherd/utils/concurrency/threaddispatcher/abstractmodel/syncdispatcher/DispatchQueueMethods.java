package shepherd.utils.concurrency.threaddispatcher.abstractmodel.syncdispatcher;

import java.util.Deque;

interface DispatchQueueMethods<I , T , QT> {

    boolean putInSyncQueue(I id, T data, Deque<QT> queue);
    void addToHandlerQueue(SyncDispatchSequence<I, T, QT> s);
}
