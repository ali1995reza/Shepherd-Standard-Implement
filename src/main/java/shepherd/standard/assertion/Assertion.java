package shepherd.standard.assertion;

public final class Assertion {

    private final static class AssertionException extends RuntimeException{

        private AssertionException(String e)
        {
            super(e);
        }

        private AssertionException(Throwable e)
        {
            super(e);
        }

        private AssertionException()
        {
            super();
        }

    }

    private final static void throwAssertionException(String e)
    {
        throw new AssertionException(e);
    }


    public final static void ifTrue(String message , boolean condition)
    {
        if(condition)
            throwAssertionException(message);
    }

    public final static void ifFalse(String message , boolean condition)
    {
        if(!condition)
            throwAssertionException(message);
    }


    public final static void ifOneNull(String message , Object ... objects)
    {
        for(Object object:objects)
        {
            ifTrue(message , object==null);
        }
    }

    public final static void ifNull(String message , Object object)
    {
        ifTrue(message , object==null);
    }

    public final static void ifNullString(String message , String ... strings)
    {
        for(String string:strings)
        {
            ifTrue(message , (string==null||string.isEmpty()));
        }
    }

    public final static void ifNotNull(String message , Object ... objects)
    {
        for(Object object:objects)
        {
            ifTrue(message , object!=null);
        }
    }
}
