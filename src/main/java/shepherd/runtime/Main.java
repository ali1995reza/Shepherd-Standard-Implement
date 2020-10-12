package shepherd.runtime;

public class Main {

    public final static void main(String[] a) throws Exception
    {
        if(a[0].equalsIgnoreCase("h"))
        {
            Helper.main(new String[]{});
        }else if(a[0].equalsIgnoreCase("n"))
        {
            NodeTest.main(new String[]{});
        }
    }
}
