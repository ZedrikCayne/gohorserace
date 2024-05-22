package com.justaddhippopotamus.ghr.server.types;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

public class RedisSet extends RedisType {
    public static final char prefix = '(';
    private final Set<RESPBulkString> value = new HashSet<>();

    public RedisSet() {

    }

    public RedisSet(Set<RESPBulkString> other) {
        value.addAll(other);
    }

    public Set<RESPBulkString> keys() {
        return new HashSet<>(value);
    }

    public synchronized List<RESPBulkString> pop(int count) {
        List<RESPBulkString> returnValue = value.stream().limit(count).collect(Collectors.toList());
        returnValue.stream().forEach(value::remove);
        return returnValue;
    }

    public synchronized RedisSet diff(List<RedisSet> sets) {
        return RedisType.atomicAllStatic(sets,RedisSet.class, all -> {
            RedisSet returnValue = new RedisSet(this);
            for (RedisSet redisSet : all) {
                returnValue.value.removeAll(redisSet.value);
                if (returnValue.value.isEmpty()) break;
            }
            return returnValue;
        });
    }
    public synchronized RESPArray sismember(List<RESPBulkString> members) {
        int len = members.size();
        RESPArray returnValue = new RESPArray(len);
        for(int i = 0; i < len; ++i ) {
            returnValue.addInteger(value.contains(members.get(i))?1:0);
        }
        return returnValue;
    }

    public synchronized boolean contains(RESPBulkString member) {
        return value.contains(member);
    }

    @Override
    public synchronized int size() {
        return value.size();
    }
    public synchronized RESPBulkString pop() {
        Iterator<RESPBulkString> iter = value.iterator();
        if( iter.hasNext() ) {
            RESPBulkString returnValue = iter.next();
            value.remove(returnValue);
            return returnValue;
        } else {
            return null;
        }
    }

    public synchronized boolean remove(RESPBulkString key) {
        return value.remove(key);
    }

    public synchronized int removeAll(Collection<RESPBulkString> keys) {
        int count = 0;
        for (RESPBulkString key : keys) {
            if(value.remove(key))++count;
        }
        return count;
    }

    public synchronized Set<RESPBulkString> getSet() {
        return value;
    }

    public synchronized int add(Collection<RESPBulkString> addThis) {
        int currentSize = value.size();
        value.addAll(addThis);
        return value.size() - currentSize;
    }

    public synchronized int add(final RedisSet other) {
        if( other == null ) return value.size();
        synchronized (other) {
            value.addAll(other.value);
        }
        return value.size();
    }

    public synchronized int add(RESPBulkString add) {
        value.add(add);
        return value.size();
    }

    public synchronized List<RESPBulkString> rand(int count) {
        boolean distinct = count>0;
        int myCount = distinct?count:-count;
        int len = value.size();
        if( len == 0 ) {
            return new ArrayList<>();
        }
        Random r = new Random();
        if( distinct ) {
            if( myCount > len ) {
                return new ArrayList<>(value);
            } else {
                boolean workDown = myCount > (len/2);
                if( workDown ) myCount = len - myCount;
                LinkedList<RESPBulkString> taken = new LinkedList<>();
                LinkedList<RESPBulkString> take = new LinkedList<>(value);
                for( int i = 0; i < myCount; ++i ) {
                    taken.add(take.remove(r.nextInt(len-i)));
                }
                return workDown?take:taken;
            }
        } else {
            List<RESPBulkString> full = new ArrayList<>(value);
            List<RESPBulkString> taken = new ArrayList<>(myCount);
            for( int i = 0; i < myCount; ++i ) {
                taken.add(full.get(r.nextInt(len)));
            }
            return taken;
        }
    }

    public RedisSet(InputStream is) throws IOException {
        readFrom(is);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(prefix);
        wireType(IRESP.RESPVersion.RESP2).publishTo(os);
    }

    @Override
    public void readFrom(InputStream is) throws IOException {
        RESPArray ra = RESPArray.readFull(is);
        int len = ra.size();
        for(int i = 0; i < ra.size(); ++i ) {
            value.add(ra.rbsAt(i));
        }
    }

    @Override
    public synchronized IRESP wireType(IRESP.RESPVersion v) {
        return new RESPArray(this);
    }

    @Override
    public void copyFrom(RedisType other) {
        super.copyFrom(other);
        RedisSet set = (RedisSet)other;
        //Sharing the values is okay since they are immutable.
        value.addAll(set.value);
    }
    @Override
    public <T extends RedisType> T copy(Class<T> type) {
        RedisSet returnValue = new RedisSet(this);
        return (T)returnValue;
    }
    public RedisSet(RedisSet other) {
        copyFrom(other);
    }

    @Override
    public String type() { return "set"; }
}
