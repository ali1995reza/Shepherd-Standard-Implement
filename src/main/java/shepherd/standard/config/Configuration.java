package shepherd.standard.config;

import shepherd.standard.assertion.Assertion;
import shepherd.api.config.*;
import shepherd.api.logger.Logger;
import shepherd.api.logger.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Configuration implements IConfiguration {





    private final static Object nullValue = new Object();

    private final Map<ConfigurationKey, Object> configs;
    private final Map<ConfigurationKey, ConfigurationChangeController> controllers;
    private final Map<String, ConfigurationKey> configKeysIndex;
    private final Map<String , Configuration> subConfigs;
    private final Object _sync = new Object();
    private List<ConfigChangeListener> listeners;
    private final IConfiguration undefinableConfiguration;
    private final String name;
    private final Logger logger;
    private final IConfiguration parent;

    public Configuration(String name)
    {
        this(name , null);
    }

    private Configuration(String name , IConfiguration parent)
    {
        this.name = name;
        this.parent = parent;
        configs = new ConcurrentHashMap<>();
        controllers = new ConcurrentHashMap<>();
        subConfigs = new ConcurrentHashMap<>();
        configKeysIndex = new ConcurrentHashMap<>();
        listeners = new ArrayList<>();
        undefinableConfiguration = UndefinableConfiguration.wrap(this);
        logger = LoggerFactory.factory().getLogger(this);
    }



    @Override
    public <T> boolean defineConfiguration(ConfigurationKey<T> configName, T defaultValue, ConfigurationChangeController controller) {

        Assertion.ifNull("controller can not be null" , controller);
        boolean success = false;

        synchronized (_sync) {

            if(configs.get(configName)==null && configKeysIndex.get(configName.name())==null)
            {
                //so we can add

                configs.put(configName ,defaultValue==null?nullValue:defaultValue);
                controllers.put(configName , controller);
                configKeysIndex.put(configName.name() , configName);
                success = true;
                logger.information("Config [{}] defined successfully" , configName.name());
            }
        }

        if(success)
        {
            callOnConfigDefined(configName , defaultValue);
        }else {
            logger.information("Config with this name [{}] already exists" , configName.name());
        }


        return success;
    }


    private <T> ConfigurationChangeResult<T> doSet(ConfigurationKey<T> confKey, T value){
        checkNotNull(confKey , "config name can not be null");
        checkNotNull(value , "config value can not be null");
        Object oldVal = null;
        ConfigurationChangeResult<T> result = null;
        synchronized (_sync)
        {
            oldVal = configs.get(confKey);

            if(oldVal==null)
            {
                //no config defined
                return ConfigChangeResult.configNotDefinedResult(
                        confKey
                );
            }

            oldVal = oldVal==nullValue?null:oldVal;
            try {
                result =
                        (ConfigurationChangeResult<T>) approveChange(confKey, oldVal, value);
            }catch (Throwable e)
            {
                return ConfigChangeResult
                        .newErrorChange(
                                e ,
                                confKey ,
                                oldVal ,
                                value
                        );
            }
            if(!result.success())
            {
                return ConfigChangeResult.newErrorChange(
                        result.error() ,
                        confKey ,
                        oldVal ,
                        value
                );
            }

            configs.put(confKey , value);
        }

        if(oldVal==null)
        {
            callOnConfigSet(confKey , value);
        }else
        {
            callOnConfigChanged(confKey , oldVal , value);
        }


        return result;
    }

    @Override
    public <T> ConfigurationChangeResult<T> set(ConfigurationKey<T> confKey, T value) {

        ConfigurationChangeResult<T> result = doSet(confKey , value);

        logger.information("{} changed , result = {}" , confKey.name() , result);

        return result;
    }

    public <T> ConfigurationChangeResult<T> doSet(String config, T value) {
        Configuration conf = findConfigByPath(config);
        if(conf==null)return null;
        String[] confPathParts = config.split("/");
        String confKey = confPathParts[confPathParts.length-1];
        ConfigurationKey key = conf.configKeysIndex.get(confKey);
        if(key==null)
            return ConfigChangeResult.configNotDefinedResult(confKey);

        return conf.set(key , value);
    }

    @Override
    public <T> ConfigurationChangeResult<T> set(String config, T value) {

        ConfigurationChangeResult<T> result = doSet(config , value);

        logger.information("{} changed , result = {}" , config , result);

        return result;
    }

    @Override
    public <T> T get(ConfigurationKey<T> confKey) {
        Object val = configs.get(confKey);
        if(val==null || val==nullValue)
            return null;

        return (T)val;
    }

    @Override
    public <T> T get(ConfigurationKey<T> confKey, T defaultValue) {
        Object val = configs.get(confKey);
        if(val==null || val==nullValue)
            return defaultValue;

        return (T)val;
    }

    @Override
    public <T> T get(String configName) {
        Configuration conf = findConfigByPath(configName);
        String[] confPathParts = configName.split("/");
        String confKey = confPathParts[confPathParts.length-1];
        ConfigurationKey key = conf.configKeysIndex.get(confKey);
        if(key==null)return null;
        return (T)get(key);
    }

    @Override
    public <T> T get(String configName, T defaultValue) {
        Configuration conf = findConfigByPath(configName);
        String[] confPathParts = configName.split("/");
        String confKey = confPathParts[confPathParts.length-1];
        ConfigurationKey key = conf.configKeysIndex.get(confKey);
        if(key==null)return defaultValue;
        return (T)get(key , defaultValue);
    }

    @Override
    public <T> boolean remove(ConfigurationKey<T> confKey) {
        Object removed = null;
        synchronized (_sync) {
            removed = configs.remove(confKey);
        }

        if(removed==null)
            return false;

        callOnConfigRemoved(confKey , removed==nullValue?null:removed);

        return true;
    }



    private final void callOnConfigRemoved(ConfigurationKey confKey , Object removedValue)
    {
        for(ConfigChangeListener listener:listeners)
        {
            try{
                listener.onConfigRemoved(confKey , removedValue);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }

    private final void callOnConfigChanged(ConfigurationKey confKey , Object oldValue , Object newValue)
    {
        for(ConfigChangeListener listener:listeners)
        {
            try{
                listener.onConfigChanged(confKey , oldValue , newValue);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }

    private final void callOnConfigSet(ConfigurationKey confKey , Object newValue)
    {
        for(ConfigChangeListener listener:listeners)
        {
            try{
                listener.onConfigSet(confKey , newValue);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }

    private final void callOnConfigDefined(ConfigurationKey confKey , Object defaultValue)
    {
        for(ConfigChangeListener listener:listeners)
        {
            try{
                listener.onConfigDefined(confKey , defaultValue);
            }catch (Throwable e)
            {
                e.printStackTrace();
            }
        }
    }

    private final Object approveChange(ConfigurationKey confKey , Object oldValue , Object newValue)
    {
        return controllers.get(confKey).approveChange(this , confKey , oldValue , newValue);
    }


    @Override
    public void addConfigChangeListener(ConfigChangeListener listener) {
        synchronized (_sync) {
            ArrayList<ConfigChangeListener> newList = copyListeners();
            newList.add(listener);
            listeners = newList;
        }
    }

    @Override
    public void removeConfigChangeListener(ConfigChangeListener listener) {
        synchronized (_sync) {
            ArrayList<ConfigChangeListener> newList = copyListeners();
            newList.remove(listener);
            listeners = newList;
        }
    }

    @Override
    public synchronized IConfiguration createSubConfiguration(String name) {

        if(subConfigs.get(name)!=null)
            throw new IllegalStateException("a sub config with ["+name+"] already exist");

        Configuration configuration = new Configuration(this.name+"/"+name );

        subConfigs.put(name , configuration);
        return configuration;
    }

    @Override
    public IConfiguration subConfiguration(String name) {
        Configuration configuration = subConfigs.get(name);

        if(configuration==null)
            return null;

        return configuration.asUndefinableConfiguration();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public IConfiguration asUndefinableConfiguration() {
        return undefinableConfiguration;
    }

    @Override
    public IConfiguration parent() {
        return parent;
    }


    private final Configuration findConfigByPath(String conf)
    {
        String[] split = conf.split("/");

        if(split.length==1)
            return this;

        Configuration configuration = this;
        for(int i=0;i<split.length-1;i++)
        {
            configuration = configuration.subConfigs.get(split[i]);
            if(configuration==null)return null;
        }

        return configuration;
    }



    private ArrayList<ConfigChangeListener> copyListeners()
    {
        ArrayList<ConfigChangeListener> newList = new ArrayList<>();
        for(int i=0;i<listeners.size();i++)
        {
            newList.add(listeners.get(i));
        }

        return newList;
    }

    private final static void checkNotNull(Object o , String msg)
    {
        if(o ==null)
            throw new NullPointerException(msg);
    }


    @Override
    public String toString() {
        return toString(3);
    }


    public final String toString(int shiftPerSub)
    {
        return toString(name , 0 , shiftPerSub);
    }

    private final String toString(String confName , int startShift , int shiftPerSub)
    {
        StringBuffer buffer = new StringBuffer();
        buffer.append(confName+"{\n");

        for(ConfigurationKey key : configs.keySet())
        {
            buffer.append(shift(startShift+shiftPerSub , key.name()+" : " , false));
            Object value = configs.get(key);
            buffer.append(shift(startShift+shiftPerSub+key.name().length()+3 ,
                    value==null || value==nullValue ?"<NOT_SET>":value.toString() , true));
            buffer.append(" , \n");
        }

        for(String subConfigName:subConfigs.keySet())
        {
            buffer.append(shift(startShift+shiftPerSub , subConfigName+" : " , false));
            Configuration configuration = subConfigs.get(subConfigName);
            buffer.append(configuration.toString("" ,
                    startShift+shiftPerSub+subConfigName.length()+3 ,
                    shiftPerSub));
            buffer.append(" , \n");
        }

        buffer.delete(buffer.length()-4 , buffer.length()-2);
        buffer.append(shift(startShift , "}" , false));
        return buffer.toString();
    }

    private final static String shift(int cells , String str , boolean dontShiftFirstLine)
    {
        if(cells<=0) return str;

        StringBuffer buffer = new StringBuffer();
        String shift = "";
        for(int i=0;i<cells;i++)
        {
            shift+=" ";
        }

        String[] split = str.split("\n");

        for(int i=0;i<split.length;i++)
        {

            if(i==0 && !dontShiftFirstLine)
                buffer.append(shift);

            buffer.append(split[i]);

            if(i<split.length-1)buffer.append("\n");
        }

        return buffer.toString();
    }

    private final static String shift(int cells , String str)
    {
        return shift(cells, str , true);
    }
}
