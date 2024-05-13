package com.justaddhippopotamus.ghr.server.types;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.RESPArray;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

public class RedisSet extends RedisType {
    public static final char prefix = '(';
    private final Set<String> value = new HashSet<>();

    public RedisSet() {

    }

    public RedisSet(Set<String> other) {
        value.addAll(other);
    }

    public Set<String> keys() {
        return new HashSet<>(value);
    }

    public synchronized List<String> pop(int count) {
        List<String> returnValue = value.stream().limit(count).collect(Collectors.toList());
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
    public synchronized RESPArray sismember(List<String> members) {
        int len = members.size();
        RESPArray returnValue = new RESPArray(len);
        for(int i = 0; i < len; ++i ) {
            returnValue.addInteger(value.contains(members.get(i))?1:0);
        }
        return returnValue;
    }

    public synchronized boolean contains(String member) {
        return value.contains(member);
    }

    public synchronized int size() {
        return value.size();
    }
    public synchronized String pop() {
        Iterator<String> iter = value.iterator();
        if( iter.hasNext() ) {
            return iter.next();
        } else {
            return null;
        }
    }

    public synchronized boolean remove(String key) {
        return value.remove(key);
    }

    public synchronized int removeAll(Collection<String> keys) {
        int count = 0;
        for (String key : keys) {
            if(value.remove(key))++count;
        }
        return count;
    }

    public synchronized Set<String> getSet() {
        return value;
    }

    public synchronized int add(Collection<String> addThis) {
        value.addAll(addThis);
        return value.size();
    }

    public synchronized int add(final RedisSet other) {
        if( other == null ) return value.size();
        synchronized (other) {
            value.addAll(other.value);
        }
        return value.size();
    }

    public synchronized int add(String add) {
        value.add(add);
        return value.size();
    }

    public synchronized List<String> rand(int count) {
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
                LinkedList<String> taken = new LinkedList<>();
                LinkedList<String> take = new LinkedList<>(value);
                for( int i = 0; i < myCount; ++i ) {
                    taken.add(take.remove(r.nextInt(len-i)));
                }
                return workDown?take:taken;
            }
        } else {
            List<String> full = new ArrayList<>(value);
            List<String> taken = new ArrayList<>(myCount);
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
            value.add(ra.stringAt(i));
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
