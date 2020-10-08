package shepherd.standard.datachannel.standard;

import sun.nio.ch.IOUtil;

import java.lang.reflect.Field;

public class IOVDetector {


    public final static int detect()
    {
        int val;
        try{

            Field IOV_Field = IOUtil.class.getDeclaredField("IOV_MAX");
            IOV_Field.setAccessible(true);
            int fieldValue = IOV_Field.getInt(null);
            if(fieldValue>=1)
            {
                val = fieldValue;

            }else
            {
                val = 1;
            }

        }catch (Throwable e)
        {
            val = -1;
        }

        return val;
    }
}
