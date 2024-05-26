package com.justaddhippopotamus.ghr.server.types;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.RESP.RESPMap;
import com.justaddhippopotamus.ghr.server.Utils;
import org.checkerframework.checker.units.qual.A;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

public class RedisSortedSet extends RedisType {
    public static final char prefix = '<';
    private static final Compy<Integer> GT_INT     = (a, b) -> a>b;
    private static final Compy<Integer> GTE_INT    = (a, b) -> a>=b;
    private static final Compy<Integer> LT_INT     = (a, b) -> a<b;
    private static final Compy<Integer> LTE_INT    = (a, b) -> a<=b;
    private static final Compy<Double>  GT_DOUBLE  = (a,b) -> a>b;
    private static final Compy<Double>  GTE_DOUBLE = (a,b) -> a>=b;
    private static final Compy<Double>  LT_DOUBLE  = (a,b) -> a<b;
    private static final Compy<Double>  LTE_DOUBLE = (a,b) -> a<=b;
    private static final Compy<String>  GT_STRING  = (a,b) -> a.compareTo(b)>0;
    private static final Compy<String>  COMP_TRUE  = (a,b) -> true;
    private static final Compy<String>  COMP_FALSE = (a,b) -> false;
    private static final Compy<String>  GTE_STRING = (a,b) -> a.compareTo(b)>=0;
    private static final Compy<String>  LT_STRING  = (a,b) -> a.compareTo(b)<0;
    private static final Compy<String>  LTE_STRING = (a,b) -> a.compareTo(b)<=0;

    //Implementation of the sorted set (
    public static class SetValue implements Comparable<SetValue> {
        public String key;
        public double score;
        public int rank;

        public SetValue(String key, double score) {
            this.key = key;
            this.score = score;
            this.rank = 0;
        }

        public SetValue(SetValue o) {
            this.key = o.key;
            this.score = o.score;
            this.rank = o.rank;
        }

        public int compareScore(double value) {
            return Double.compare(this.score, value);
        }

        public int hashCode() { return key.hashCode(); }

        @Override
        public int compareTo(SetValue other) {
            int returnValue = Double.compare(score,other.score);
            return returnValue == 0 ? key.compareTo(other.key) : returnValue;
        }
    }
    protected final Map<String,SetValue> value = new HashMap<>();
    protected final TreeSet<SetValue> sortedValue = new TreeSet<>();
    protected final TreeSet<String> sortedKeys = new TreeSet<>();

    public final Set<SetValue> getSortedValues() { return sortedValue; }

    public synchronized Double getScore(String key) {
        SetValue sv = value.getOrDefault(key,null);
        if( sv == null ) return null;
        return sv.score;
    }

    public synchronized Set<String> keys() {
        return new HashSet<>(value.keySet());
    }

    public synchronized List<SetValue> rand(int count, boolean distinct) {
        Set<String> keys = value.keySet();

        if (count >= value.size() && distinct) {
            return sortedValue.stream().collect(Collectors.toList());
        }

        String[] keyArray = keys.toArray(new String[0]);
        Random rand = new Random();
        List<SetValue> returnValue = new ArrayList<>(count);
        Set<String> taken = new HashSet<>(keyArray.length);
        for (int i = 0; i < count; ++i) {
            String picked = keyArray[Math.abs(rand.nextInt()) % keyArray.length];
            if (distinct && !taken.add(picked)) --i;
            else returnValue.add(value.get(picked));
        }
        return returnValue;
    }

    public synchronized double incr(String key, double amount) {
        SetValue sv = value.getOrDefault(key, null);
        if (sv == null) {
            addRaw(key,amount);
            return amount;
        } else {
            sortedValue.remove(sv);
            sv.score += amount;
            sortedValue.add(sv);
            return sv.score;
        }
    }

    public synchronized double incr(Map<String,Double> stuffToAdd) {
        double newValue = 0.0d;
        for( String key : stuffToAdd.keySet() ) {
            double amount = stuffToAdd.get(key);
            newValue = incr(key, amount);
        }
        return newValue;
    }
    public synchronized int add(Map<String,Double> elements, boolean NX, boolean XX, boolean GT, boolean LT, boolean CH ) {
        int numAdded = 0;
        int numChanged = 0;
        for( String k : elements.keySet() ) {
            double v = elements.get(k);
            SetValue previous = value.getOrDefault(k,null);
            if( previous != null) {
                if( !NX ) {
                    int compared = previous.compareScore(v);
                    if( (GT&&compared<0)
                      ||(LT&&compared>0)
                      ||(!GT&&!LT) ) {
                        if( compared != 0 ) {
                            numChanged++;
                            sortedValue.remove(previous);
                            previous.score = v;
                            sortedValue.add(previous);
                        }
                    }
                }
            } else {
                if( !XX ) {
                    ++numAdded;
                    ++numChanged;
                    addRaw(k,v);
                }
            }
        }
        return CH?numChanged:numAdded;
    }

    public synchronized boolean remove(String key) {
        SetValue sv = value.getOrDefault(key,null);
        if( sv != null) {
            sortedValue.remove(sv);
            value.remove(key);
            sortedKeys.remove(key);
            return true;
        }
        return false;
    }

    public Map<String,Double> toMap() {
        Map<String,Double> returnValue = new HashMap<>(value.size());
        for( SetValue v : value.values() ) {
            returnValue.put(v.key,v.score);
        }
        return returnValue;
    }

    public synchronized List<RESPBulkString> toArray(boolean WITHSCORES) {
        List<RESPBulkString> returnValue = new ArrayList<>(size() * (WITHSCORES ? 2 : 1));
        for (RedisSortedSet.SetValue sortedValue : sortedValue ) {
            returnValue.add(new RESPBulkString(sortedValue.key));
            if (WITHSCORES) {
                returnValue.add(new RESPBulkString(Utils.doubleToStringRedisStyle(sortedValue.score)));
            }
        }
        return returnValue;
    }

    public synchronized int removeAll(Collection<String> keys) {
        int counter = 0;
        for( String key : keys ) if(remove(key)) ++counter;
        return counter;
    }

    public synchronized Set<String> getSet() {
        return value.keySet();
    }

    private void addRaw(String key,double val) {
        SetValue sv = new SetValue(key,val);
        value.put(key,sv);
        sortedValue.add(sv);
        sortedKeys.add(key);
    }

    private List<String> range(String start, String stop, boolean REV, boolean LIMIT, int offset, int count, boolean WITHSCORES ) {
        int len = sortedValue.size();
        int startIndex = Utils.rangeIndex(start,len,REV);
        boolean startInclusive = Utils.rangeStringInclusive(start);
        int stopIndex = Utils.rangeIndex(stop,len,REV);
        boolean stopInclusive = Utils.rangeStringInclusive(start);
        List<String> returnValue = new ArrayList<>(sortedValue.size() * (WITHSCORES?2:1));
        Iterator<SetValue> iter;
        int taken = 0;
        int skipped = 0;
        int currentIndex;
        int step;
        Compy<Integer> startComp;
        Compy<Integer> stopComp;
        if( REV ) {
            step = -1;
            currentIndex = len - 1;
            iter = sortedValue.descendingIterator();
            if( startInclusive )
                startComp = LTE_INT;
            else
                startComp = LT_INT;
            if( stopInclusive )
                stopComp = LT_INT;
            else
                stopComp = LTE_INT;
        } else {
            step = 1;
            currentIndex = 0;
            iter = sortedValue.iterator();
            if( startInclusive )
                startComp = GTE_INT;
            else
                startComp = GT_INT;
            if( stopInclusive )
                stopComp = GT_INT;
            else
                stopComp = GTE_INT;
        }
        while( iter.hasNext() ) {
            SetValue next = iter.next();
            if( stopComp.accept(currentIndex,stopIndex) )
                break;
            if( startComp.accept(currentIndex,startIndex) ) {
                if( LIMIT ) {
                    if( skipped < offset ) {
                        ++skipped;
                        continue;
                    }
                }
                returnValue.add(next.key);
                if( WITHSCORES )
                    returnValue.add(Utils.doubleToStringRedisStyle(next.score));
                if( LIMIT ) {
                    ++taken;
                    if( taken >= count )
                        break;
                }
            }
            currentIndex += step;
        }
        return returnValue;
    }
    private List<String> rangeLex(String start, String stop, boolean REV, boolean LIMIT, int offset, int count, boolean WITHSCORES ) {
        int len = sortedKeys.size();
        boolean startInclusive = Utils.rangeStringInclusive(start);
        boolean stopInclusive = Utils.rangeStringInclusive(stop);
        String startIndex = Utils.rangeString(start);
        String stopIndex = Utils.rangeString(stop);
        int step, currentIndex, skipped = 0,taken = 0;
        Iterator<String> iter;
        Compy<String> startComp;
        Compy<String> stopComp;
        if( REV ) {
            iter = sortedKeys.descendingIterator();
            if( startIndex.compareTo("+") == 0 ) {
                startComp = COMP_TRUE;
            } else {
                if (startInclusive)
                    startComp = LTE_STRING;
                else
                    startComp = LT_STRING;
            }
            if( stopIndex.compareTo("-") == 0 ) {
                stopComp = COMP_FALSE;
            } else {
                if (stopInclusive)
                    stopComp = LT_STRING;
                else
                    stopComp = LTE_STRING;
            }
        } else {
            iter = sortedKeys.iterator();
            if( startIndex.compareTo("-") == 0 ) {
                startComp = COMP_TRUE;
            } else {
                if (startInclusive)
                    startComp = GTE_STRING;
                else
                    startComp = GT_STRING;
            }
            if( stopIndex.compareTo("+") == 0 ) {
                stopComp = COMP_FALSE;
            } else {
                if (stopInclusive)
                    stopComp = GT_STRING;
                else
                    stopComp = GTE_STRING;
            }
        }
        List<String> returnValue = new ArrayList<>(len*(WITHSCORES?2:1));
        while( iter.hasNext() ) {
            String next = iter.next();
            if( stopComp.accept(next,stopIndex) )
                break;
            if( startComp.accept(next,startIndex) ) {
                if( LIMIT ) {
                    if( skipped < offset ) {
                        ++skipped;
                        continue;
                    }
                }
                returnValue.add(next);
                if( WITHSCORES ) {
                    returnValue.add(Utils.doubleToStringRedisStyle(value.get(next).score));
                }
                if( LIMIT ) {
                    ++taken;
                    if( taken >= count )
                        break;
                }
            }
        }
        return returnValue;
    }
    private List<String> rangeScore(String start, String stop, boolean REV, boolean LIMIT, int offset, int count, boolean WITHSCORES) {
        int len = sortedKeys.size();
        boolean startInclusive = Utils.rangeStringInclusive(start);
        boolean stopInclusive = Utils.rangeStringInclusive(stop);
        double startIndex = Utils.rangeDouble(start);
        double stopIndex = Utils.rangeDouble(stop);
        int step, currentIndex, skipped = 0,taken = 0;
        Iterator<SetValue> iter;
        Compy<Double> startComp;
        Compy<Double> stopComp;
        if( REV ) {
            step = -1;
            currentIndex = len - 1;
            iter = sortedValue.descendingIterator();
            if( startInclusive )
                startComp = LTE_DOUBLE;
            else
                startComp = LT_DOUBLE;
            if( stopInclusive )
                stopComp = LT_DOUBLE;
            else
                stopComp = LTE_DOUBLE;
        } else {
            step = 1;
            currentIndex = 0;
            iter = sortedValue.iterator();
            if( startInclusive )
                startComp = GTE_DOUBLE;
            else
                startComp = GT_DOUBLE;
            if( stopInclusive )
                stopComp = GT_DOUBLE;
            else
                stopComp = GTE_DOUBLE;
        }
        List<String> returnValue = new ArrayList<>(len*(WITHSCORES?2:1));
        while( iter.hasNext() ) {
            SetValue next = iter.next();
            if( stopComp.accept(next.score,stopIndex) )
                break;
            if( startComp.accept(next.score,startIndex) ) {
                if( LIMIT ) {
                    if( skipped < offset ) {
                        ++skipped;
                        continue;
                    }
                }
                returnValue.add(next.key);
                if( WITHSCORES ) {
                    returnValue.add(Utils.doubleToStringRedisStyle(next.score));
                }
                if( LIMIT ) {
                    ++taken;
                    if( taken >= count )
                        break;
                }
            }
        }
        return returnValue;
    }
    public synchronized List<String> range(String start, String stop,
                                           boolean BYLEX, boolean BYSCORE, boolean REV,
                                           boolean LIMIT,
                                           int offset, int count,
                                           boolean WITHSCORES) {
        if( BYLEX ) {
            return rangeLex(start,stop,REV,LIMIT,offset,count,WITHSCORES);
        } else if ( BYSCORE ){
            return rangeScore(start,stop,REV,LIMIT,offset,count,WITHSCORES);
        } else {
            return range(start,stop,REV,LIMIT,offset,count,WITHSCORES);
        }
    }

    @Override
    public synchronized int size() {
        return value.size();
    }

    @Override
    public synchronized boolean isEmpty() { return value.size() == 0; }

    public int count(String min, String max) {
        Compy<Double> minComp = Utils.rangeStringInclusive(min)?GTE_DOUBLE:GT_DOUBLE;
        Compy<Double> maxComp = Utils.rangeStringInclusive(max)?LTE_DOUBLE:LT_DOUBLE;
        double minDouble = Utils.rangeDouble(min);
        double maxDouble = Utils.rangeDouble(max);
        Iterator<SetValue> iterator = sortedValue.iterator();
        int returnValue = 0;
        SetValue sv = null;
        while( iterator.hasNext() ) {
            sv = iterator.next();
            if( minComp.accept(sv.score,minDouble) ) {
                break;
            }
        }
        if( sv == null ) return 0;
        while( maxComp.accept( sv.score, maxDouble ) ) {
            returnValue++;
            if( !iterator.hasNext() )
                break;
            sv = iterator.next();
        }
        return returnValue;
    }

    public synchronized SetValue rank(String member, boolean reverse) {
        if( !value.containsKey(member) ) {
            return null;
        }
        int current = 0;
        //Get the member, we're going to compare it later so we're not doing
        //a bunch of string compares every iteration.
        SetValue sv = value.get(member);
        Iterator<SetValue> iter = reverse?sortedValue.descendingIterator():sortedValue.iterator();
        while( iter.hasNext() ) {
            SetValue currentSV = iter.next();
            //This is the default == which means that we're comparing whether or not
            //sv is exactly the same object as currentSV.
            if( sv == currentSV ) {
                //We're returning a copy, as next operation might cause this to
                //change rank or score and we are throwing this over a thread wall.
                SetValue returnValue = new SetValue(currentSV);
                returnValue.rank = current;
                return returnValue;
            }
            ++current;
        }
        return null;
    }

    public synchronized IRESP wireType(IRESP.RESPVersion v) {
        if( v == IRESP.RESPVersion.RESP2 )
            return new RESPArray(this);
        else
            return new RESPMap(this);
    }

    public RedisSortedSet() {

    }
    public RedisSortedSet(InputStream is) throws IOException {
        readFrom(is);
    }

    public RedisSortedSet(Map<String,Double> map) {
        add(map,false,false,false,false,false);
    }

    public RedisSortedSet(List<String> values) {
        int len = values.size();
        for( int i = 0; i < len; i += 2 ) {
            addRaw(values.get(i),Utils.stringToDoubleRedisStyle(values.get(i+1)));
        }
    }

    @Override
    public synchronized void writeTo(OutputStream os) throws IOException {
        os.write(prefix);
        writeTTL(os);
        RESPArray ra = new RESPArray(this);
        ra.publishTo(os);
    }

    @Override
    public void readFrom(InputStream is) throws IOException {
        readTTL(is);
        RESPArray ra = RESPArray.readFull(is);
        for( int i = 0; i < ra.size(); i+=2 ) {
            String key = ra.stringAt(i);
            double val = ra.doubleAt(i+1);
            addRaw(key,val);
        }
    }
    @Override
    public void copyFrom(RedisType other) {
        super.copyFrom(other);
        RedisSortedSet sortedSet = (RedisSortedSet)other;
        //In the copy, we don't mind sharing the keys because they are not mutable.
        sortedSet.atomic( (RedisSortedSet rs) -> {
            for (SetValue setValue : rs.value.values()) {
                addRaw(setValue.key,setValue.score);
            }
        });
    }

    public synchronized List<SetValue> popmin(int count) {
        Iterator<SetValue> iter = sortedValue.iterator();
        int taken = 0;
        List<SetValue> returnValue = new ArrayList<>(count);
        while(iter.hasNext() && taken < count) {
            returnValue.add(iter.next());
            ++taken;
        }
        iter = returnValue.iterator();
        while( iter.hasNext() ) remove(iter.next().key);
        return returnValue;
    }

    public synchronized List<SetValue> popmax(int count) {
        Iterator<SetValue> iter = sortedValue.descendingIterator();
        int taken = 0;
        List<SetValue> returnValue = new ArrayList<>(count);
        while(iter.hasNext() && taken < count) {
            returnValue.add(iter.next());
            ++taken;
        }
        iter = returnValue.iterator();
        while( iter.hasNext() ) remove(iter.next().key);
        return returnValue;
    }
    @Override
    public <T extends RedisType> T copy(Class<T> type) {
        RedisSortedSet returnValue = new RedisSortedSet(this);
        return (T)returnValue;
    }
    public RedisSortedSet(RedisSortedSet other) {
        if( other != null ) copyFrom(other);
    }

    @Override
    public String type() { return "zset"; }
}
