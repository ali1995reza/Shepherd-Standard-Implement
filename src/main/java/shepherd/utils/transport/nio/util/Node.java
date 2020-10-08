package shepherd.utils.transport.nio.util;

public class Node<T> {

    private T data;
    private Node<T> nextNode;
    private Node<T> previousNode;
    private boolean removed;
    private final NodeList list;
    Node(T data , NodeList l)
    {
        this.data = data;
        this.list = l;
    }


    void setNextNode(Node<T> nextNode) {
        this.nextNode = nextNode;
    }


    void setPreviousNode(Node<T> previousNode) {
        this.previousNode = previousNode;
    }

    public Node<T> next() {
        return nextNode;
    }

    public Node<T> previous() {
        return previousNode;
    }

    public T data() {
        return data;
    }

    void setRemoved()
    {
        removed = true;
    }

    public boolean isRemoved()
    {
        return removed;
    }

    public boolean isRelatedToList(NodeList l)
    {
        return list == l;
    }
}
