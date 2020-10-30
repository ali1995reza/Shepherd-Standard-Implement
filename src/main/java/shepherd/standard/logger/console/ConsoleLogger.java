package shepherd.standard.logger.console;

import com.sun.org.apache.xerces.internal.impl.dv.xs.AnyURIDV;
import shepherd.api.logger.LogConfig;
import shepherd.api.logger.Logger;
import shepherd.api.logger.LoggerConfigure;
import shepherd.api.logger.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static shepherd.standard.logger.console.Utils.getStackTraceAsString;
import static shepherd.api.logger.Logger.LogLevel.*;

public class ConsoleLogger implements Logger {

    public static final LoggerConfigure FULL_LOG_CONFIG =
            new LoggerConfigure() {
                private final LogLevel[] levels =
                        {
                                ERROR ,
                                EXCEPTION ,
                                WARNING ,
                                INFORMATION ,
                                DEBUG ,
                                STACK_TRACE
                        };
                @Override
                public LogConfig config(Object object) {

                    String name = object==null?
                            "Console-Logger":
                            (object instanceof Class)?
                                    ((Class) object).getSimpleName():
                                    object.getClass().getSimpleName();

                    return LogConfig.newConfig()
                            .addLevels(levels)
                            .setName(name)
                            .build();
                }
            };

    public static final LoggerConfigure FULL_LOG_CONFIG_WITHOUT_STACK_TRACE =
            new LoggerConfigure() {
                private final LogLevel[] levels =
                        {
                                ERROR ,
                                EXCEPTION ,
                                WARNING ,
                                INFORMATION ,
                                DEBUG
                        };
                @Override
                public LogConfig config(Object object) {

                    String name = object==null?
                            "Console-Logger":
                            (object instanceof Class)?
                                    ((Class) object).getSimpleName():
                                    object.getClass().getSimpleName();

                    return LogConfig.newConfig()
                            .addLevels(levels)
                            .setName(name)
                            .build();
                }
            };


    public static class Factory extends LoggerFactory
    {


        private final static LogLevel[] DEFAULT_LEVEL = {INFORMATION , ERROR , EXCEPTION , WARNING};

        private LoggerConfigure configure;
        private final static LogConfig DEFAULT_CONFIG =
                LogConfig.newConfig()
                        .addLevels(DEFAULT_LEVEL)
                        .setName("Console-Logger")
                        .build();

        @Override
        public LoggerFactory setLoggerConfigure(LoggerConfigure configure) {
            this.configure = configure;
            return this;
        }

        @Override
        public Logger getLogger(Object object) {

            LoggerConfigure configure = this.configure;

            LogConfig config = configure==null?
                    DEFAULT_CONFIG:configure.config(object);

            config = config==null?DEFAULT_CONFIG:config;

            return new ConsoleLogger(
                    config.levels() ,
                    object ,
                    config.name()
            );
        }
    }







    private final static String STACK_TRACE_TAG = "[ STACK_TRACE ]";
    private final static String WARNING_TAG     = "[   WARNING   ]";
    private final static String INFORMATION_TAG = "[ INFORMATION ]";
    private final static String ERROR_TAG       = "[    ERROR    ]";
    private final static String EXCEPTION_TAG   = "[  EXCEPTION  ]";
    private final static String DEBUG_TAG       = "[    DEBUG    ]";


    private final static String STACK_TRACE_COLOR =
            ConsoleColors.CYAN_BACKGROUND+ConsoleColors.BLUE_BOLD;

    private final static Calendar calendar = Calendar.getInstance();
    private final static SimpleDateFormat formatter =
            new SimpleDateFormat("dd MMMMM yyyy EEEEE HH:mm:ss.SSS");

    private final LogLevel[] level;
    private final boolean logStackTrace;
    private final Object object;
    private final int logLevelCode;
    private final String name;

    public ConsoleLogger(LogLevel[] level, Object o , String name)
    {
        this.name = name==null||name.isEmpty()?"null":name;
        this.level = level;
        logLevelCode = LogLevel.createLogLevelId(level);
        logStackTrace = LogLevel.contains(logLevelCode , STACK_TRACE);
        object = o;


    }

    @Override
    public LogLevel[] logLevel() {
        return level;
    }

    @Override
    public boolean containsLevel(LogLevel level) {

        return LogLevel.contains(logLevelCode , level);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void debug(Object o) {
        if(!containsLevel(DEBUG))
            return;

        String log = buildLog(DEBUG_TAG , o);
        log +="\n"+getStackTraceAsString(STACK_TRACE_COLOR ,STACK_TRACE_TAG)+"\n";
        log+="\n";
        System.out.print(log);
    }

    @Override
    public void debug(String str, Object... objects) {
        if(!containsLevel(DEBUG))
            return;

        String log = buildLog(DEBUG_TAG , replaceArgs(str , objects));
        if(logStackTrace)
            log +="\n"+getStackTraceAsString(STACK_TRACE_COLOR ,STACK_TRACE_TAG)+"\n";
        log+="\n";
        System.out.print(log);
    }

    @Override
    public void error(Object o) {
        if(!containsLevel(ERROR))
            return;

        String log = buildLog(ERROR_TAG , o);
        if(logStackTrace)
            log +="\n"+getStackTraceAsString(STACK_TRACE_COLOR ,STACK_TRACE_TAG)+"\n";
        log+="\n";
        System.out.print(log);
    }

    @Override
    public void error(String str, Object... objects) {
        if(!containsLevel(ERROR))
            return;

        String log = buildLog(ERROR_TAG , replaceArgs(str , objects));
        if(logStackTrace)
            log +="\n"+getStackTraceAsString(STACK_TRACE_COLOR ,STACK_TRACE_TAG)+"\n";
        log+="\n";
        System.out.print(log);
    }

    @Override
    public void error(Throwable e) {
        error("error !" , e);
    }

    @Override
    public void error(String s, Throwable e) {
        if(!containsLevel(ERROR))
            return;

        String log = buildLog(ERROR_TAG , s);
        log+="\n"+ Utils.toString(e)+"\n";
        System.out.println(log);
    }

    @Override
    public void information(Object o) {
        if(!containsLevel(INFORMATION))
            return;

        String log = buildLog(INFORMATION_TAG, o);
        if(logStackTrace)
            log +="\n"+getStackTraceAsString(STACK_TRACE_COLOR ,STACK_TRACE_TAG)+"\n";
        log+="\n";
        System.out.print(log);
    }

    @Override
    public void information(String str, Object... objects) {
        if(!containsLevel(INFORMATION))
            return;

        String log = buildLog(INFORMATION_TAG , replaceArgs(str , objects));
        if(logStackTrace)
            log +="\n"+getStackTraceAsString(STACK_TRACE_COLOR ,STACK_TRACE_TAG)+"\n";
        log+="\n";
        System.out.print(log);
    }

    @Override
    public void warning(Object o) {
        if(!containsLevel(WARNING))
            return;

        String log = buildLog(WARNING_TAG , o);
        if(logStackTrace)
            log +="\n"+getStackTraceAsString(STACK_TRACE_COLOR ,STACK_TRACE_TAG)+"\n";
        log+="\n";
        System.out.print(log);
    }

    @Override
    public void warning(String str, Object... objects) {
        if(!containsLevel(WARNING))
            return;

        String log = buildLog(WARNING_TAG , replaceArgs(str , objects));
        if(logStackTrace)
            log +="\n"+getStackTraceAsString(STACK_TRACE_COLOR ,STACK_TRACE_TAG)+"\n";
        log+="\n";
        System.out.print(log);
    }

    @Override
    public void warning(Throwable e) {
        warning("warning !" , e);
    }

    @Override
    public void warning(String s, Throwable e) {
        if(!containsLevel(ERROR))
            return;

        String log = buildLog(WARNING_TAG , s);
        log+="\n"+Utils.toString(e)+"\n";
        System.out.println(log);
    }

    @Override
    public void exception(Object o) {
        if(!containsLevel(EXCEPTION))
            return;

        String log = buildLog(EXCEPTION_TAG , o);
        if(logStackTrace)
            log +="\n"+getStackTraceAsString(STACK_TRACE_COLOR ,STACK_TRACE_TAG)+"\n";
        log+="\n";
        System.out.print(log);
    }

    @Override
    public void exception(String str, Object... objects) {
        if(!containsLevel(EXCEPTION))
            return;

        String log = buildLog(EXCEPTION_TAG , replaceArgs(str , objects));
        if(logStackTrace)
            log +="\n"+getStackTraceAsString(STACK_TRACE_COLOR ,STACK_TRACE_TAG)+"\n";
        log+="\n";
        System.out.print(log);
    }

    @Override
    public void exception(Throwable e) {
        exception("exception !" , e);
    }

    @Override
    public void exception(String s, Throwable e) {
        if(!containsLevel(EXCEPTION))
            return;

        String log = buildLog(EXCEPTION_TAG , s);
        log+="\n"+Utils.toString(e)+"\n";
        System.out.println(log);
    }


    private final String buildLog(String color , String level , Object value)
    {
        return color+level+" - [ "+ name +" - "+formatter.format(new Date())+" ]"+
                ConsoleColors.RESET+" : "+value+"\n";
    }

    private final String buildLog(String level , Object value)
    {
        return buildLog(
                selectColorByTag(level),
                level ,
                value
        );
    }

    private final static String selectColorByTag(String tag)
    {
        if(tag.equals(INFORMATION_TAG))
        {
            return ConsoleColors.GREEN_BACKGROUND+ConsoleColors.BLACK_BOLD;
        }else if(tag.equals(WARNING_TAG))
        {
            return ConsoleColors.YELLOW_BACKGROUND_BRIGHT+ConsoleColors.BLACK_BOLD;
        }else if(tag.equals(EXCEPTION_TAG))
        {
            return ConsoleColors.PURPLE_BACKGROUND+ConsoleColors.BLACK_BOLD;
        }else if(tag.equals(ERROR_TAG))
        {
            return ConsoleColors.RED_BACKGROUND+ConsoleColors.BLACK_BOLD;
        }else if(tag.equals(DEBUG_TAG))
        {
            return ConsoleColors.CYAN_BACKGROUND+ConsoleColors.BLUE_BOLD;
        }

        return ConsoleColors.BLACK;
    }


    private final static String replaceArgs(String s  , Object ... args)
    {
        return Utils.replace(s , args);
    }

}
