package shepherd.runtime;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Set;

public class MaxThreadUsage extends Thread {

    @Override
    public void run() {
        while (true)
        {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            handle();
        }
    }

    private void handle()
    {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        Thread maxConsumer = null;
        long usage = 0;

        for(Thread thread:threads)
        {
            long l = threadMXBean.getThreadCpuTime(thread.getId());
            if(l>usage)
            {
                maxConsumer = thread;
                usage = l;
            }
        }

        System.out.println(maxConsumer + " : "+usage);
    }
}