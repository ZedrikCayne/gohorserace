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
