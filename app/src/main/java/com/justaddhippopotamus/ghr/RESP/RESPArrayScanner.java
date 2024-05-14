package com.justaddhippopotamus.ghr.RESP;

import com.justaddhippopotamus.ghr.server.types.RedisString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RESPArrayScanner {
    private final RESPArray commands;
    private final int len;
    private int currentIndex;
    private boolean isSubcommand;
    public void throwError() {
        throw new RuntimeException(commandString() + ": Bad arguments");
    }

    private String commandString() {
        if( !isSubcommand ) return command();
        return command() + " " + subcommand();
    }

    ///Helper for parsing commands.
    public RESPArrayScanner(RESPArray commands) {
        this.commands = commands;
        this.len = commands.size();
        this.isSubcommand = false;
        this.currentIndex = 1;
    }
    public RESPArrayScanner(RESPArray commands, boolean isSubcommand ) {
        this.commands = commands;
        this.len = commands.size();
        this.isSubcommand = isSubcommand;
        currentIndex = isSubcommand?2:1;
    }

    public int size() { return len; }

    public String command() { return commands.argAt(0); }
    public String subcommand() { return commands.argAt(1); }
    public boolean commandIs(String... what) { return Arrays.asList(what).contains(commands.argAt(0)); }
    public boolean subcommandIs(String... what) { return Arrays.asList(what).contains(commands.argAt(1)); }
    public void setCurrent(int currentIndex) {
        this.currentIndex = currentIndex;
    }

    public int takeInt() { return commands.intAt(currentIndex++); }

    public long takeLong() { return commands.longAt(currentIndex++); }

    public double takeDouble() { return commands.doubleAt(currentIndex++); }

    public List<Double> takeDoubles(int num) {
        List<Double> returnValue = new ArrayList<>(num);
        for( int i = 0; i < num; ++i ) returnValue.add( takeDouble() );
        return returnValue;
    }
    public String string() { return commands.stringAt(currentIndex++); }

    public RESPBulkString bulkString() { return commands.rbsAt(currentIndex++); }

    public RedisString redisString() { return commands.redisStringAt(currentIndex++); }

    public boolean hasNext() { return currentIndex < len; }

    public String key() { return string(); }

    public void backUp() { currentIndex--; }
    public String takeArg() { return commands.argAt(currentIndex++); }

    public String peekArg() { return commands.argAtMaybe(currentIndex); }

    ///Grab the numkeys (Assumes that the current index is an integer,
    ///followed by exactly the number of 'keys'
    public List<String> getNumKeys() {
        final int numkeys = takeInt();
        final List<String> returnValue = new ArrayList<>(numkeys);
        for( int i = 0; i < numkeys; ++i ) {
            returnValue.add( key() );
        }
        return returnValue;
    }

    ///Takes the next element if it matches one of the arguments. Returns empty string otherwise and doesn't move the cursor.
    public String argOneOf(String... compareTo) {
        String maybeArg = peekArg();
        if( maybeArg.isEmpty() ) return maybeArg;
        for (String s : compareTo) {
            if (s.compareTo(maybeArg) == 0) {
                ++currentIndex;
                return maybeArg;
            }
        }
        return "";
    }

    public String argOneOfRequired(String... compareTo) {
        String returnValue = argOneOf(compareTo);
        if(returnValue.equals("")) throwError();
        return returnValue;
    }

    //Returns true if argument is the second, false if it is the first
    public boolean argIsRequired(String returnsFalse, String returnsTrue) {
        String returnValue = argOneOfRequired( returnsFalse, returnsTrue );
        return returnsTrue.compareTo(returnValue) == 0;
    }

    ///Takes the element if it matches the argument and returns true, false otherwise and doesn't move the cursor
    public boolean argIs(String compareTo) { return !argOneOf(compareTo).equals(""); }

    public void errorOnRemains() { if( hasNext() ) throwError(); }

    public List<String> remainingElementsRequired(int limit) {
        List<String> returnValue = commands.elementsFromIndexLimit(currentIndex,limit);
        currentIndex = len;
        return returnValue;
    }

    public List<String> remainingElementsOptional() {
        if( currentIndex < len ) {
            List<String> returnValue = commands.elementsFromIndex(currentIndex);
            currentIndex = len;
            return returnValue;
        } else {
            return new ArrayList<>();
        }
    }

    public List<RESPBulkString> remainingBulkStrings() {
        if( currentIndex < len ) {
            List<RESPBulkString> returnValue = new ArrayList<>(len - currentIndex);
            for( int i = currentIndex; i < len; ++i ) {
                returnValue.add(commands.rbsAt(i));
            }
            currentIndex = len;
            return returnValue;
        } else {
            return new ArrayList<>();
        }
    }
}
