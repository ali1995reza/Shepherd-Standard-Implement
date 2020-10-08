package shepherd.utils.concurrency.common;

public class ThreadConfig {

    private final int priority;
    private final boolean daemon;
    private final ClassLoader classLoader;
    private final String name;
    private final Thread.UncaughtExceptionHandler exceptionHandler;


    public ThreadConfig(int priority, boolean daemon, ClassLoader classLoader, String name, Thread.UncaughtExceptionHandler exceptionHandler)
    {
        this.priority = priority;
        this.daemon = daemon;
        this.classLoader = classLoader;
        this.name = name;
        this.exceptionHandler = exceptionHandler;
    }

    public ThreadConfig(int priority , boolean daemon , ClassLoader loader , String name)
    {
        this(priority , daemon , loader , name , null);
    }


    public ThreadConfig(int priority , boolean daemon , ClassLoader loader)
    {
        this(priority , daemon , loader , null , null);
    }


    public ThreadConfig(int priority , boolean daemon ,  String name)
    {
        this(priority , daemon , null , name , null);
    }

    public ThreadConfig(int priority , boolean daemon)
    {
        this(priority , daemon , null , null, null);
    }

    public String getName() {
        return name;
    }

    public int getPriority() {
        return priority;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public Thread.UncaughtExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    public boolean isDaemon() {
        return daemon;
    }
}
