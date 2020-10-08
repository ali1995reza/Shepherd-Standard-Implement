package shepherd.standard.logger.console;

import shepherd.api.logger.Logger;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    private final static Pattern ARG_PATTERN = Pattern.compile(Logger.ARG_TOKEN_REGX_STRING);
    private final static String NULL_ARG = "<NULL>";

    /**
     * a method to get stack trace as string to print for log
     * @return Stack Trace as String
     */

    public static String getStackTraceAsString(String color , String tag)
    {
        //do handleBy it lease !
        String trace = color+tag+ ConsoleColors.RESET+" - {\n\n";
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for(int i = elements.length-1 ; i>=3 ; i--)
        {
            StackTraceElement element = elements[i];
            trace += "  at "+element.getClassName()+"."+element.getMethodName();
            trace += "("+element.getFileName()+":"+element.getLineNumber()+")";
            trace += "\n";
        }
        return trace+"\n}"+" - "+color+tag+ConsoleColors.RESET;
    }

    public final static String replace(String str , Object ... all)
    {
        if(all==null ||  all.length==0)
            return str;

        Matcher matcher = ARG_PATTERN.matcher(str);

        StringBuffer buffer = new StringBuffer().append(str);

        int index = 0 ;
        int increased = 0;
        while (matcher.find())
        {
            if(index>=all.length)
                break;

            Object o = all[index++];

            String s = o==null?NULL_ARG:o.toString();
            s = s==null?NULL_ARG:s;

            buffer.replace(matcher.start()+increased , matcher.end()+increased , s);

            increased+= s.length()-2;
        }

        return buffer.toString();
    }

    public final static String toString(Throwable e , String encoding)
    {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            PrintStream stream = new PrintStream(byteArrayOutputStream ,
                    true ,
                    encoding);
            e.printStackTrace(stream);
            stream.close();
            byteArrayOutputStream.close();
            return ConsoleColors.RED+new String(byteArrayOutputStream.toByteArray() , encoding)+ConsoleColors.RESET;
        }catch (Throwable ex)
        {
            return "";
        }
    }

    public final static String toString(Throwable e)
    {
        return toString(e , "UTF-8");
    }
}
