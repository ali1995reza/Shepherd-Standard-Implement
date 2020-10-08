package shepherd.standard.config;

import shepherd.api.config.ConfigurationChangeResult;
import shepherd.api.config.ConfigurationKey;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public final class ConfigChangeResult<T> implements ConfigurationChangeResult<T> {




    private final static String toString(Throwable e , String encoding)
    {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            PrintStream stream = new PrintStream(byteArrayOutputStream ,
                    true ,
                    encoding);
            e.printStackTrace(stream);
            stream.close();
            byteArrayOutputStream.close();
            return new String(byteArrayOutputStream.toByteArray() , encoding);
        }catch (Throwable ex)
        {
            return "";
        }
    }

    private final static String throwableErrorMessage(Throwable e , ConfigurationKey key , Object current , Object newVal)
    {
        return new StringBuffer("An unhandled exception occurs when change configuration [")
                .append(key.name())
                .append("] from [")
                .append(current==null?"null":current.toString())
                .append("] to [")
                .append(newVal==null?"null":newVal.toString())
                .append("]")
                .append(" : \r\n")
                .append(toString(e , "UTF-8"))
                .toString();
    }

    private final static String errorMessage(String err , ConfigurationKey key , Object current, Object newVal)
    {
        return new StringBuffer("An error occurs when change configuration [")
                .append(key.name())
                .append("] from [")
                .append(current==null?"null":current.toString())
                .append("] to [")
                .append(newVal==null?"null":newVal.toString())
                .append("]")
                .append(" : \r\n")
                .append(err)
                .toString();
    }


    public final static <T> ConfigChangeResult<T> newSuccessResult(T value)
    {
        return new ConfigChangeResult<>(true , null , value);
    }

    public final static <T> ConfigChangeResult<T> newFailResult(String err)
    {
        return new ConfigChangeResult<>(false , err , null);
    }



    final static <T> ConfigChangeResult<T> newSuccessChange(T val)
    {
        return new ConfigChangeResult(true , null , val);
    }

    final static <T> ConfigChangeResult<T> newErrorChange(String error ,
                                                                 ConfigurationKey key ,
                                                                 Object current ,
                                                                 Object newVal)
    {
        return new ConfigChangeResult(false , errorMessage(error, key, current, newVal) , null);
    }

    final static <T> ConfigChangeResult<T> newErrorChange(Throwable error , ConfigurationKey key ,
                                                                 Object current ,
                                                                 Object newVal)
    {
        return new ConfigChangeResult(false , throwableErrorMessage(error , key , current , newVal) , null);
    }


    final static ConfigChangeResult configNotDefinedResult(String confName)
    {
        return new ConfigChangeResult(false ,
                "config not defined for ["+confName+"]" , null);
    }


    final static ConfigChangeResult configNotDefinedResult(ConfigurationKey key)
    {
        return configNotDefinedResult(key.name());
    }

    private final boolean success;
    private final String error;
    private final T newValue;

    private ConfigChangeResult(boolean success, String error, T newValue) {
        this.success = success;
        this.error = error;
        this.newValue = newValue;
    }




    @Override
    public boolean success() {
        return success;
    }

    @Override
    public String error() {
        return error;
    }

    @Override
    public T newValue() {
        return newValue;
    }

    @Override
    public String toString() {
        return "{" +
                "success=" + success +
                ", error='" + error + '\'' +
                ", newValue=" + newValue +
                '}';
    }
}
