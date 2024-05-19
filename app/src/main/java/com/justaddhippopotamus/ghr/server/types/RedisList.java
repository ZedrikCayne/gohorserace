package com.justaddhippopotamus.ghr.server.types;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.Server;
import com.justaddhippopotamus.ghr.server.WorkItem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

public class RedisList extends RedisType {
    public static final char prefix = '[';
    public RedisList() {
        value = new LinkedList<>();
    }
    public RedisList(InputStream is) throws IOException {
        readFrom(is);
    }

    public List<String> value() {
        return value;
    }
    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(prefix);
        RESPArray ra = new RESPArray(value);
        ra.publishTo(os);
    }

    @Override
    public void readFrom(InputStream is) throws IOException {
        RESPArray ra = RESPArray.readFull(is);
        value = new LinkedList<>(ra.toStringList());
    }

    @Override
    public synchronized IRESP wireType(IRESP.RESPVersion v) {
        return null;
    }

    private LinkedList<String> value;


    public synchronized int push(String s) {
        value.addFirst(s);
        return value.size();
    }

    public synchronized int push(Collection<String> s) {
        for( String aS : s ) {
            value.addFirst(aS);
        }
        return value.size();
    }
    public synchronized int rpush(String s) {
        value.addLast(s);
        return value.size();
    }

    public synchronized  int rpush(Collection<String> s) {
        for( String aS : s ) {
            value.addLast(aS);
        }
        return value.size();
    }
    public synchronized String pop() {
        return value.removeFirst();
    }
    public synchronized String rpop() {
        return value.removeLast();
    }
    public synchronized List<String> pop(int count) {
        int toPull = Math.min(count, value.size());
        if( toPull == 0 )
            return null;
        List<String> returnValue = new ArrayList<>(toPull);
        for( int i = 0; i < toPull; ++i ) {
            returnValue.add( value.removeFirst() );
        }
        return returnValue;
    }

    public synchronized List<Integer> pos(String what, int count, int maxlen, int rank ) {
        boolean reverse = rank < 0;
        int realRank = Math.abs(rank);
        List<Integer> returnValue = new ArrayList<>();
        Iterator<String> iter;
        if( reverse ) {
            iter = value.descendingIterator();
        } else {
            iter = value.iterator();
        }
        int taken = 0;
        int found = 0;
        int length = 0;
        boolean hasCount = count > 0;
        boolean hasMaxLen = maxlen > 0;
        int len = value.size();
        while( iter.hasNext() ) {
            String that = iter.next();
            if( that.compareTo(what) == 0 ) {
                ++found;
                if( found >= realRank) {
                    if( reverse ) returnValue.add(len - length - 1);
                    else returnValue.add(length);
                    ++taken;
                }
                if( hasCount && taken >= count )
                    break;
            }
            length++;
            if( hasMaxLen && length >= maxlen )
                break;
        }
        return returnValue;
    }
    public synchronized List<String> rpop(int count) {
        int toPull = Math.min(count, value.size());
        if( toPull == 0 )
            return null;
        List<String> returnValue = new ArrayList<>(toPull);
        for( int i = 0; i < toPull; ++i ) {
            returnValue.add( value.removeLast() );
        }
        return returnValue;
    }

    @Override
    public synchronized boolean canBeReaped() {
        return (value.isEmpty() && blocked.isEmpty());
    }

    public synchronized boolean isEmpty() {
        return value.isEmpty();
    }
    public synchronized String move(RedisList destination, boolean leftFrom, boolean leftDestination ) {
        if( value.size() > 0 ) {
            synchronized( destination ) {
                String takenFrom = leftFrom? pop():rpop();
                if (leftDestination) {
                    destination.push(takenFrom);
                } else {
                    destination.rpush(takenFrom);
                }
                return takenFrom;
            }
        } else {
            return null;
        }
    }

    public synchronized String index(int index) {
        String returnValue = null;
        if( Math.abs(index) < value.size() ) {
            if( index > 0 )
                returnValue = value.get(index);
            else
                returnValue = value.get(value.size() + index);
        }
        return returnValue;
    }

    public synchronized int insert(boolean before, String pivot, String element) {
        int listSize = value.size();
        for( int i = 0; i < listSize; ++i ) {
            if( value.get(i).compareTo(pivot) == 0 ) {
                if( before ) {
                    value.add(i,element);
                } else {
                    value.add(i+1,element);
                }
                return listSize + 1;
            }
        }
        return -1;
    }

    @Override
    public synchronized int size() {
        return value.size();
    }

    @Override
    public void copyFrom(RedisType other) {
        super.copyFrom(other);
    }
    @Override
    public <T extends RedisType> T copy(Class<T> type) {
        RedisList returnValue = new RedisList(this);
        return (T)returnValue;
    }
    public RedisList(RedisList other) {
        copyFrom(other);
    }

    public static boolean isNullOrEmpty(RedisList that) {
        return that == null || that.value.size() == 0;
    }

    public synchronized RESPArray range(int start, int stop) {
        int len = value.size();
        int realStart = realIndex(start,len);
        int realStop = realIndex(stop,len);
        ++realStop;//Increment because subList does not include the 'last' index
        if(realStart < 0) realStart = 0;
        if(realStop > len) realStop = len;
        if( realStart >= realStop ) return new RESPArray();
        Iterator<String> iter = value.iterator();
        RESPArray returnValue = new RESPArray(realStop - realStart);
        for( int i = 0; i < realStop; ++i ) {
            String s = iter.next();
            if( i < realStart ) continue;
            returnValue.addString(s);
        }
        return returnValue;
        //return new ArrayList<>(value.subList(realStart,realStop));
    }

    public synchronized int remove(String what, int count) {
        List<Integer> stuffToRemove = new ArrayList<>();
        int numRemoved = 0;
        if( count == 0 ) {
            while( value.remove(what) ) ++numRemoved;
        } else if ( count > 0 ) {
            while( numRemoved < count && value.removeFirstOccurrence(what) ) ++numRemoved;
        } else {
            int countPositive = -count;
            while( numRemoved < countPositive && value.removeLastOccurrence(what) ) ++numRemoved;
        }
        return numRemoved;
    }

    public synchronized boolean set(String what, int index) {
        int len = value.size();
        int realIndex = realIndex(index,len);
        if( realIndex >= len ) return true;
        value.set(index,what);
        return false;
    }

    public synchronized void trim(int start, int stop) {
        int len = value.size();
        int realStart = realIndex(start,len);
        int realStop = realIndex(stop,len);
        if( realStart > realStop  || realStart > len ) {
            value = new LinkedList<>();
        } else {
            for( int i = 0; i < realStart; ++i )
                value.removeFirst();
            for( int i = realStop + 1; i < len; ++i )
                value.removeLast();
        }
    }

    @Override
    public String type() { return "list"; }
}
