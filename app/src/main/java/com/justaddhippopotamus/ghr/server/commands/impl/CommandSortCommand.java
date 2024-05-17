
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.*;
import com.justaddhippopotamus.ghr.server.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSortCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern
        //  ...]] [ASC | DESC] [ALPHA] [STORE destination]
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        String pattern = commands.argIs("BY")?commands.string():null;
        int offset = 0;
        int count = -1;
        if( commands.argIs("LIMIT") ) {
            offset = commands.takeInt();
            count = commands.takeInt();
        }
        List<String> patterns = new ArrayList<>();
        while( commands.argIs("GET") ) {
            patterns.add( commands.string() );
        }
        boolean DESC = commands.argIs("DESC");
        commands.argIs("ASC");
        boolean ALPHA = commands.argIs("ALPHA");
        String destination = commands.argIs("STORE")?commands.string():null;
        RedisType sortTarget = item.getMainStorage().fetchRO(key,RedisType.class);
        SortableComparator<Sortable> comparator = selectComparator(pattern,ALPHA,DESC);

        RedisType toSort = item.getMainStorage().fetchRO(key,RedisType.class);

        if( toSort instanceof RedisList || toSort instanceof RedisSet || toSort instanceof RedisSortedSet ) {
            List<Sortable> sortables = makeSortables(item.getMainStorage(),toSort,pattern,ALPHA,comparator);
            if( comparator != NOSORT )
            sortables.sort(null);
            List<String> returnValue = new ArrayList<>(sortables.size() * (patterns.size() + 1) );
            for( var s : sortables ) {
                if (!patterns.isEmpty()) {
                    for( var p : patterns ) {
                        if( p.charAt(0) == '#' ) returnValue.add(s.originalValueAsString);
                        else returnValue.add(
                                RedisString.normalized(
                                        item.getMainStorage().fetchRO(
                                                p.replace("*",s.originalValueAsString),RedisString.class)));
                    }
                } else {
                    returnValue.add(s.originalValueAsString);
                }
            }
            item.whoFor.queue(RESPArray.RESPArrayWithNulls(returnValue,item.whoFor.clientRESPVersion),item.order);
        } else {
            item.whoFor.queueSimpleError("Need a 'list' 'set' or 'sorted set'", item.order);
        }
    }

    private SortableComparator<Sortable> selectComparator(String pattern, boolean alpha, boolean desc) {
        return (pattern==null||pattern.contains("*"))?
                alpha?(desc?ALPHA_R:ALPHA_F):(desc?DOUBLE_R:DOUBLE_F):
                NOSORT;
    }

    private String fixRedisString(RedisString what,boolean alpha) {
        if( what == null ) return null;
        else return what.toString();
    }
    private Sortable sortableFor(TypeStorage storage, String value, Double dVal, String pattern, String elementPart, boolean alpha, SortableComparator<Sortable> comparator) {
        String keyPart = pattern==null?null:pattern.replace("*",value);
        String weightedString = null;
        if( keyPart != null ) {
            if( elementPart != null ) {
                RedisHash rh = storage.fetchRO(keyPart, RedisHash.class);
                if( rh == null ) {
                    weightedString = null;
                } else {
                    weightedString = fixRedisString(rh.get(elementPart),alpha);
                }
            } else {
                weightedString = fixRedisString(storage.fetchRO(keyPart,RedisString.class),alpha);
            }
        }
        Sortable sv = new Sortable(value,weightedString,comparator);
        if(comparator != NOSORT && !alpha) {
            if(dVal == null)
                sv.setValue();
            else
                sv.dVal = dVal;
        }
        return sv;
    }
    private List<Sortable> makeSortables(TypeStorage storage, RedisType toConvert, String pattern, boolean alpha, SortableComparator<Sortable> comparator) {
        boolean hasPattern = pattern != null;
        boolean hasElement = hasPattern && pattern.contains("->");
        String keyPart = hasPattern?pattern.replaceFirst(".*->",""):null;
        String elementPart = hasElement?pattern.replaceFirst( "->.*", ""):null;
        ArrayList<Sortable> returnValue = new ArrayList<Sortable>(toConvert.size());
        toConvert.atomic( t -> {
            if( toConvert instanceof RedisList ) {
                RedisList rl = (RedisList)t;
                List<String> values = rl.value();
                values.forEach(v -> returnValue.add(sortableFor(storage,v,null,keyPart,elementPart,alpha,comparator)));
            } else if( toConvert instanceof RedisSet ) {
                RedisSet rs = (RedisSet)t;
                rs.keys().forEach(v->returnValue.add(sortableFor(storage,v,null,keyPart,elementPart,alpha,comparator)));
            } else if( toConvert instanceof RedisSortedSet ) {
                RedisSortedSet rss = (RedisSortedSet)t;
                rss.getSortedValues().forEach(v->returnValue.add(sortableFor(storage,v.key,v.score,keyPart,elementPart,alpha,comparator)));
            }
        });
        return returnValue;
    }

    @FunctionalInterface
    private interface SortableComparator<T> {
        int accept(T a, T b);
    }
    private static class Sortable implements Comparable<Sortable> {
        String originalValueAsString;
        String weightedValueAsString;
        double dVal = 0.0d;
        SortableComparator<Sortable> comparator;

        public Sortable(String element, String weighted, SortableComparator<Sortable> c) {
            originalValueAsString = element;
            weightedValueAsString = weighted;
            comparator = c;
        }

        public void setValue() {
            dVal = weightedValueAsString==null?Utils.stringToDoubleRedisStyle(originalValueAsString)
                    :Utils.stringToDoubleRedisStyle(weightedValueAsString);
        }
        @Override
        public int compareTo(Sortable o) {
            return comparator.accept(this,o);
        }
    }
    private static final SortableComparator<Sortable> DOUBLE_F = (a,b) -> Double.compare(a.dVal,b.dVal);
    private static final SortableComparator<Sortable> DOUBLE_R = (a,b) -> Double.compare(b.dVal,a.dVal);
    private static final SortableComparator<Sortable> ALPHA_F = (a,b) -> a.compareTo(b);
    private static final SortableComparator<Sortable> ALPHA_R = (a,b) -> b.compareTo(a);
    private static final SortableComparator<Sortable> NOSORT = (a,b) -> 0;
}
