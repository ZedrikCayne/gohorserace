package com.justaddhippopotamus.ghr.server;

import java.io.PrintStream;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Logger {
    @FunctionalInterface
    private interface LogExpensive {
        String accept();
    }
    private static class LogItem {
        protected String item;
        protected Throwable t;
        protected LogItem(String item) {
            this.item = item;
            this.t = null;
        }

        protected LogItem(String item, Throwable t) {
            this.item = item;
            this.t = t;
        }
    }
    private static class PrintStreamLogWriter extends GoDog {
        protected PrintStreamLogWriter(PrintStream out) {
            this.out = out;
            goDogGo();
        }
        @Override
        public boolean stop() {
           boolean wasRunning  = super.stop();

           return wasRunning;
        }

        @Override
        public void run() {
            super.run();
            while(running) {
                try {
                    var toLog = messages.take();
                    var sb = sb();
                    sb.append(toLog.item);
                    out.println(sb.toString());
                    if( toLog.t != null ) toLog.t.printStackTrace(out);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            mainLoopCompleted();
        }
        PrintStream out;
    }

    private static PrintStreamLogWriter stdioWriter = new PrintStreamLogWriter(System.out);
    private static final int NUM_STRING_BUILDERS = 256;
    private static int currentBuilder = 0;
    private static StringBuilder [] builders = new StringBuilder[ NUM_STRING_BUILDERS ];
    static StringBuilder sb() {
        synchronized (builders) {
            if (builders[currentBuilder] == null) {
                builders[currentBuilder] = new StringBuilder(1024);
            }
            builders[currentBuilder].setLength(0);
            StringBuilder returnValue = builders[currentBuilder];
            currentBuilder = (currentBuilder + 1) % NUM_STRING_BUILDERS;
            return returnValue;
        }
    }
    private static ConcurrentHashMap<String,Logger> loggers = null;

    public static Logger get(String what) {
        if(loggers == null) loggers = new ConcurrentHashMap<>(256);
        return loggers.computeIfAbsent(what, s -> new Logger(s,defaultLogLevel));
    }

    public enum LogLevel implements Comparable<LogLevel> {
        LOG_DEFAULT,
        LOG_DISABLE,
        LOG_EXCEPTION,
        LOG_ERROR,
        LOG_WARNING,
        LOG_INFO,
        LOG_DEBUG,
        LOG_TRACE;

        private static LogLevel fromString(String what) {
            switch(what.toUpperCase()) {
                case "INFO":
                case "I":
                case "LOG_INFO":
                    return LOG_INFO;
                case "WARN":
                case "WARNING":
                case "W":
                case "LOG_WARNING":
                    return LOG_WARNING;
                case "TRACE":
                case "T":
                case "LOG_TRACE":
                    return LOG_TRACE;
                case "DEBUG":
                case "D":
                case "LOG_DEBUG":
                    return LOG_DEBUG;
                case "EXCEPTION":
                case "EX":
                case "LOG_EXCEPTION":
                    return LOG_EXCEPTION;
                case "ERROR":
                case "E":
                case "ERR":
                case "LOG_ERROR":
                    return LOG_ERROR;
                case "DISABLE":
                case "OFF":
                    return LOG_DISABLE;
                default:
                    throw new RuntimeException("Bad log level supplied.");
            }
        }
    }

    public void exception(String s, Throwable t) {
        if(canLog(LogLevel.LOG_EXCEPTION)) {
            raw(t,forWhom,"EXCEPTION",s);
        }
    }
    public void exception(LogExpensive e, Throwable t) {
        if(canLog(LogLevel.LOG_EXCEPTION)) {
            raw(t,forWhom,"EXCEPTION",e.accept());
        }
    }

    public void error(String s) {
        if(canLog(LogLevel.LOG_ERROR)) {
            raw(null,forWhom,"ERROR",s);
        }
    }
    public void error(LogExpensive e) {
        if(canLog(LogLevel.LOG_ERROR)) {
            raw(null,forWhom,"ERROR",e.accept());
        }
    }

    public void log(String s) {
        raw(null,forWhom,s);
    }
    public void log(LogExpensive e) {
        raw(null,forWhom,e.accept());
    }
    public void info(String s) {
        if(canLog(LogLevel.LOG_INFO)) {
            raw(null,forWhom,"INFO",s);
        }
    }
    public void info(LogExpensive e) {
        if(canLog(LogLevel.LOG_INFO)) {
            raw(null,forWhom,"INFO",e.accept());
        }
    }
    public void warn(String s) {
        if(canLog(LogLevel.LOG_WARNING)) {
            raw(null,forWhom,"WARN",s);
        }
    }
    public void warn(LogExpensive e) {
        if(canLog(LogLevel.LOG_WARNING)) {
            raw(null,forWhom,"WARN",e.accept());
        }
    }
    public void trace(String s) {
        if(canLog(LogLevel.LOG_TRACE)) {
            raw(null,forWhom,"TRACE",s);
        }
    }

    public void trace(LogExpensive e) {
        if(canLog(LogLevel.LOG_TRACE)) {
            raw(null,forWhom,"TRACE",e.accept());
        }
    }

    private void raw(Throwable t, String... strings) {
        var sb = sb();
        if( timestamped ) {
            sb.append(Instant.now());
            sb.append('|');
        }
        for (var s : strings) {
            sb.append(s);
            sb.append('|');
        }
        sb.setLength(sb.length() - 1);
        messages.add(new LogItem(sb.toString(),t));
    }
    private Logger(String forWhom, LogLevel currentLevel) {
        this.forWhom = forWhom;
        this.logLevel = currentLevel;
    }

    private boolean canLog(LogLevel what) { return currentLogLevel().compareTo(what)>=0; }
    private LogLevel currentLogLevel() { return logLevel==LogLevel.LOG_DEFAULT?defaultLogLevel:logLevel;}
    private String forWhom;
    protected LogLevel logLevel;
    private static boolean timestamped = true;
    private static LogLevel defaultLogLevel = LogLevel.LOG_INFO;
    public static void setDefaultLogLevel(LogLevel l) {
        defaultLogLevel = l;
    }

    public static void setLogLevelFor(String what, String level) {
        LogLevel l = LogLevel.fromString(level);
        get(what).logLevel(l);
    }

    public static void setDefaultLogLevel(String level) {
        defaultLogLevel = LogLevel.fromString(level);
    }

    public Logger logLevel(LogLevel l){
        logLevel = l;
        return this;
    }

    static BlockingQueue<LogItem> messages = new LinkedBlockingQueue<>();
}
