package shepherd.utils.transport.nio.util;

import java.util.function.Consumer;

public class NodeList<T> {


    private final Object _sync = new Object();
    private Node<T> firstNode;
    private Node<T> lastNode;
    private boolean consuming = false;
    public NodeList()
    {

    }

    public Node<T> add(T data)
    {
        //so synchronized
        Node<T> node = new Node<>(data , this);

        synchronized (_sync)
        {
            if(consuming)
                throw new IllegalStateException("can not add when consuming");

            if(firstNode==null)
            {
                //so no node exist
                firstNode = node;
                lastNode = node;
                return node;
            }else
            {
                node.setPreviousNode(lastNode);
                lastNode.setNextNode(node);
                lastNode = node;
                return node;
            }
        }
    }


    public void remove(Node<T> node)
    {
        if(!node.isRelatedToList(this))
            throw new IllegalStateException("this node not related to this list");

        synchronized (_sync)
        {

            if(consuming)
                throw new IllegalStateException("can not remove when consuming");

            if(node.isRemoved())
                throw new IllegalStateException("node already removed");

            if(node.previous()!=null)
            {
                node.previous().setNextNode(node.next());
            }

            if(node.next()!=null)
            {
                node.next().setPreviousNode(node.previous());
            }


            if(node==firstNode)
                firstNode = node.next();

            if(node==lastNode)
                lastNode = node.previous();

            node.setNextNode(null);
            node.setPreviousNode(null);
            node.setRemoved();
        }
    }

    public void forEach(Consumer<T> consumer)
    {
        synchronized (_sync)
        {
            consuming = true;
            Node<T> currentNode = firstNode;

            while (currentNode!=null)
            {
                try {
                    consumer.accept(currentNode.data());
                }catch (Throwable e)
                {
                    consuming = false;
                    throw e;
                }
                currentNode = currentNode.next();
            }

            consuming = false;
        }

    }

}
