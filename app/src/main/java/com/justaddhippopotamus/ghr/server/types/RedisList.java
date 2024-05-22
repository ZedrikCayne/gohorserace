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

    public List<RESPBulkString> value() {
        return value;
    }
    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(prefix);
        RESPArray ra = RESPArray.RESPArrayFromCollectionOfBulkStrings(value);
        ra.publishTo(os);
    }

    @Override
    public void readFrom(InputStream is) throws IOException {
        RESPArray ra = RESPArray.readFull(is);
        value = new LinkedList<>(ra.toBulkStringList());
    }

    @Override
    public synchronized IRESP wireType(IRESP.RESPVersion v) {
        return null;
    }

    private LinkedList<RESPBulkString> value;


    public synchronized int push(RESPBulkString s) {
        value.addFirst(s);
        return value.size();
    }

    public synchronized int push(Collection<RESPBulkString> s) {
        for( var aS : s ) {
            value.addFirst(aS);
        }
        return value.size();
    }
    public synchronized int rpush(RESPBulkString s) {
        value.addLast(s);
        return value.size();
    }

    public synchronized  int rpush(Collection<RESPBulkString> s) {
        for( var aS : s ) {
            value.addLast(aS);
        }
        return value.size();
    }
    public synchronized RESPBulkString pop() {
        return value.removeFirst();
    }
    public synchronized RESPBulkString rpop() {
        return value.removeLast();
    }
    public synchronized List<RESPBulkString> pop(int count) {
        int toPull = Math.min(count, value.size());
        if( toPull == 0 )
            return null;
        List<RESPBulkString> returnValue = new ArrayList<>(toPull);
        for( int i = 0; i < toPull; ++i ) {
            returnValue.add( value.removeFirst() );
        }
        return returnValue;
    }

    public synchronized List<Integer> pos(RESPBulkString what, int count, int maxlen, int rank ) {
        boolean reverse = rank < 0;
        int realRank = Math.abs(rank);
        List<Integer> returnValue = new ArrayList<>();
        Iterator<RESPBulkString> iter;
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
            RESPBulkString that = iter.next();
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
    public synchronized List<RESPBulkString> rpop(int count) {
        int toPull = Math.min(count, value.size());
        if( toPull == 0 )
            return null;
        List<RESPBulkString> returnValue = new ArrayList<>(toPull);
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
    public synchronized RESPBulkString move(RedisList destination, boolean leftFrom, boolean leftDestination ) {
        if( value.size() > 0 ) {
            synchronized( destination ) {
                RESPBulkString takenFrom = leftFrom? pop():rpop();
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

    public synchronized RESPBulkString index(int index) {
        RESPBulkString returnValue = null;
        if( Math.abs(index) < value.size() ) {
            if( index > 0 )
                returnValue = value.get(index);
            else
                returnValue = value.get(value.size() + index);
        }
        return returnValue;
    }

    public synchronized int insert(boolean before, RESPBulkString pivot, RESPBulkString element) {
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
        Iterator<RESPBulkString> iter = value.iterator();
        RESPArray returnValue = new RESPArray(realStop - realStart);
        for( int i = 0; i < realStop; ++i ) {
            RESPBulkString s = iter.next();
            if( i < realStart ) continue;
            returnValue.addRespElement(s);
        }
        return returnValue;
        //return new ArrayList<>(value.subList(realStart,realStop));
    }

    public synchronized int remove(RESPBulkString what, int count) {
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

    public synchronized boolean set(RESPBulkString what, int index) {
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
