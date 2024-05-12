package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.server.commands.impl.CommandZinterCommand;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Utils {

    public static Set<String> intersection(List<Set<String>> toIntersect) {
        Set<String> a = new HashSet<>();
        toIntersect.sort(Comparator.comparingInt(Set::size));
        int len = toIntersect.size();
        a.addAll(toIntersect.get(0));
        Set<String> toRemove = new HashSet<>();
        for( int i = 1; i < len; ++i ) {
            toRemove.clear();
            Set<String> b = toIntersect.get(i);
            for( String item : a ) if( !b.contains(item) ) toRemove.add(item);
            for( String item : toRemove ) a.remove(item);
            if( a.size() == 0 ) {
                break;
            }
        }
        return a;
    }

    //Inverse difference is what to remove from the initial list to get it to the
    //difference list
    public Set<String> difference(List<Set<String>> toDiff, boolean inverse) {
        Set<String> a = new HashSet<>();
        Set<String> b = inverse?new HashSet<>():null;
        a.addAll(toDiff.get(0));
        int len = toDiff.size();
        for( int i = 1; i < len; ++i ) {
            for( String item : toDiff.get(i) )
                if( a.remove(item) && inverse ) b.add(item);
            if( a.size() == 0 ) break;
        }
        return inverse?b:a;
    }

    public static List<String> getVarKeys(RESPArray commands, int numkeyIndex) {
        List<String> returnKeys = new ArrayList<>(commands.size());
        int numKeys = commands.intAt(numkeyIndex);
        for( int i = numkeyIndex + 1; i <= numkeyIndex + numKeys; ++i ) {
            returnKeys.add(commands.stringAt(i));
        }
        return returnKeys;
    }

    public static int weightsIndex(RESPArray commands, int numKeys, int numKeyIndex ) {
        return numKeys + numKeyIndex + 1;
    }
    public static int aggregateModeIndex(RESPArray commands, int weightsIndex, int numKeys, boolean hasWeights) {
        return hasWeights?weightsIndex + numKeys + 1:weightsIndex;
    }

    private static int numKeys(RESPArray commands, int numkeyIndex) {
        return commands.intAt(numkeyIndex);
    }
    public static List<Double> getWeights(RESPArray commands, int numKeys, int weightsIndex) {
        List<Double> returnWeights = null;
        if( commands.argAtMaybeIs(weightsIndex,"WEIGHTS") ) {
            returnWeights = new ArrayList<Double>(commands.size());
            for( int i = weightsIndex + 1; i <= weightsIndex + numKeys; ++i )
                returnWeights.add(commands.doubleAt(i));
        }
        return returnWeights;
    }

    public static boolean getWithScores(RESPArray commands) {
        return commands.argAtMaybeIs(commands.size() - 1,"WITHSCORES");
    }

    public static CommandZinterCommand.AGGREGATE_MODE getAggregateMode(RESPArray commands, int aggregateModeIndex ) {
        if( commands.argAtMaybeIs(aggregateModeIndex,"AGGREGATE") ) {
            switch ( commands.argAtMaybe(aggregateModeIndex + 1) ) {
                case "MIN":
                    return CommandZinterCommand.AGGREGATE_MODE.AGGREGATE_MODE_MIN;
                case "MAX":
                    return CommandZinterCommand.AGGREGATE_MODE.AGGREGATE_MODE_MAX;
                case "SUM":
                case "":
                    break;
                default:
                    throw new RuntimeException("Bad args");
            }
        }
        return CommandZinterCommand.AGGREGATE_MODE.AGGREGATE_MODE_SUM;
    }

    public static int getAggregateLimit(RESPArray commands, int aggregateIndex ) {
        int limit = 0;
        int limitIndex = 0;
        if( commands.argAtMaybeIs(aggregateIndex,"AGGREGATE") ) {
            if (commands.argAtMaybeIs(aggregateIndex + 1,"LIMIT") )
                limitIndex = aggregateIndex + 2;
            if (commands.argAtMaybeIs(aggregateIndex + 2, "LIMIT") )
                limitIndex = aggregateIndex + 3;
        }
        if( limitIndex > 0 ) {
            limit = commands.intAt( limitIndex );
        }
        return limit;
    }

    public static PathMatcher forGlob(String glob) {
        String fixedGlob = glob.replace("[^","[!");
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + fixedGlob );
        return matcher;
    }

    public static Set<String> matchesForGlob(Collection<String> input, PathMatcher matcher) {
        return input.stream().filter(p -> matcher.matches(Paths.get(p)) ).collect(Collectors.toSet());
    }
    public static Set<String> getMatches(Collection<String> input, String glob) {
        PathMatcher matcher = forGlob(glob);
        return matchesForGlob(input,matcher);
    }

    public static String doubleToStringRedisStyle(double value) {
        if( value == Double.POSITIVE_INFINITY ) {
            return "+inf";
        }
        if( value == Double.NEGATIVE_INFINITY ) {
            return "-inf";
        }
        if( value == Double.NaN ) {
            return "nan";
        }
        return String.valueOf(value);
    }

    public static boolean rangeStringInclusive(String rangeNumber) {
        switch( rangeNumber.charAt(0) ) {
            case '(':
                return false;
            case '[':
            default:
                return true;
        }
    }

    public static double rangeDouble(String rangeNumber) {
        String parseMe = rangeNumber;
        switch( rangeNumber.charAt(0) ) {
            case '-':
                return Double.NEGATIVE_INFINITY;
            case '+':
                return Double.POSITIVE_INFINITY;
            case '(':
            case '[':
                parseMe = rangeNumber.substring(1);
                break;
            default:
                break;
        }
        try {
            return stringToDoubleRedisStyle(parseMe);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Cannot parse a number out of " + rangeNumber);
        }
    }

    private static String parseableBit(String rangeString) {
        switch( rangeString.charAt(0) ) {
            case '[':
            case '(':
                return rangeString.substring(1);
            default:
                return rangeString;
        }
    }
    public static int rangeIndex(String rangeString,int max) {
        try {
            int returnValue = Integer.parseInt(parseableBit(rangeString));
            if( returnValue < 0 )
                returnValue = max + returnValue;
            return returnValue;
        } catch (NumberFormatException e) {
            throw new RuntimeException("Int not parseable of " + rangeString);
        }
    }

    public static String rangeString(String rangeString) {
        String parseMe = rangeString;
        switch( rangeString.charAt(0) ) {
            case '[':
            case '(':
                return rangeString.substring(1);
            case '-':
            case '+':
                if( rangeString.length() == 1)
                    return rangeString;
            default:
                throw new RuntimeException("Range string must start with ( or [ or be - or +");
        }
    }

    public static double stringToDoubleRedisStyle(String input) {
        double value;
        try {
            value = Double.parseDouble(input);
        } catch (NumberFormatException e) {
            switch (input.toLowerCase(Locale.US)) {
                case "inf":
                case "+inf":
                    value = Double.POSITIVE_INFINITY;
                    break;
                case "-inf":
                    value = Double.NEGATIVE_INFINITY;
                    break;
                case "nan":
                    value = Double.NaN;
                    break;
                default:
                    throw new RuntimeException("Failed to parse as double: " + input);
            }
        }
        return value;
    }
}
