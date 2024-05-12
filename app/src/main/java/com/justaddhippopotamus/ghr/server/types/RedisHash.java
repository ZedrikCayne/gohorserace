package com.justaddhippopotamus.ghr.server.types;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.RESP.RESPMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

public class RedisHash extends RedisType {
    public Map<String,RedisString> value;
    public static final char prefix = '&';
    @Override
    public synchronized void writeTo(OutputStream os) throws IOException {
        os.write(prefix);
        wireType(IRESP.RESPVersion.RESP2).publishTo(os);
    }

    @Override
    public void readFrom(InputStream is) throws IOException {
        RESPArray ra = RESPArray.readFull(is);
        int len = ra.size();
        value = new HashMap<>(len/2);
        for( int i = 0; i < len; i += 2 ) {
            putRaw(ra.stringAt(i), ra.redisStringAt(i+1));
        }
    }

    @Override
    public IRESP wireType(IRESP.RESPVersion v) {
        if( v == IRESP.RESPVersion.RESP2 ) {
            RESPArray ra = new RESPArray(this);
            return ra;
        } else {
            RESPMap rm = new RESPMap(this);
            return rm;
        }
    }

    public synchronized long incrBy(String field, long howMuch) {
        RedisString theValue = value.getOrDefault(field,null);
        if( theValue == null ) {
            theValue = new RedisString(0);
            value.put(field,theValue);
        }
        return theValue.increment(howMuch);
    }
    public synchronized RedisString incrFloat(String field, String howMuch) {
        RedisString theValue = value.getOrDefault(field,null);
        if( theValue == null ) {
            theValue = new RedisString("0.0");
            value.put(field,theValue);
        }
        return theValue.incrementFloat(howMuch);
    }

    public synchronized List<String> rand(int count, boolean withValues) {
        boolean duplicates = count < 0;
        int mycount = duplicates?-count:count;
        count = mycount;
        LinkedList<String> keyString = new LinkedList<>(keys());
        int len = keyString.size();
        if( count >= len && !duplicates ) {
            return keyString;
        }
        Random r = new Random();
        if( !duplicates && count > len / 2 ) {
            while( mycount > 0 ) {
                keyString.remove(r.nextInt(mycount));
                --mycount;
            }
        } else {
            List<String> arrayList = new ArrayList<>(keyString);
            keyString.clear();
            while( mycount > 0 ) {
                keyString.add(arrayList.get(r.nextInt(len)));
                --mycount;
            }
        }
        if( withValues ) {
            for( int i = 0; i < count; i++ ) {
                keyString.add(i*2+1, value.get(keyString.get( i * 2 )).toString());
            }
        }
        return keyString;
    }

    public synchronized Set<String> keys() {
        return value.keySet();
    }

    @Override
    public <T extends RedisType> T copy(Class<T> type) {
        return (T) new RedisHash(this);
    }

    public RedisHash(InputStream is) throws IOException {
        readFrom(is);
    }

    public RedisHash(RedisHash in) {
        value = new HashMap<>(in.value.size());
        in.value.forEach((k,v) -> value.put(k,new RedisString(v)));
    }


    public synchronized int addPairs(List<RESPBulkString> stuffToAdd, boolean NX) {
        if( stuffToAdd.size() % 2 != 0 ) {
            throw new RuntimeException("Stuff to add to a hash needs to be in pairs <field> <value>");
        }
        int len = stuffToAdd.size();
        int added = 0;
        for( int i = 0; i < len; i += 2 ) {
            String key = stuffToAdd.get(i).toString();
            if( NX && value.containsKey(key) ) continue;
            if( putRaw( key, stuffToAdd.get(i+1) ) == null ) ++added;
        }
        return added;
    }


    public synchronized int strlen(String element) {
        RedisString rs = value.getOrDefault(element,null);
        if( rs == null ) return 0;
        return rs.length();
    }

    public synchronized List<RedisString> values() {
        return new ArrayList<>(value.values());
    }

    public synchronized int del(List<String> fields) {
        int len = fields.size();
        int numRemoved = 0;
        for( int i = 0; i < len; ++i ) {
            if( value.remove(fields.get(i)) != null ) ++numRemoved;
        }
        return numRemoved;
    }


    private RedisString putRaw(String field, String fieldValue) {
        return putRaw(field,new RedisString(fieldValue));
    }

    private RedisString putRaw(String field, RedisString fieldValue) {
        return value.put(field,fieldValue);
    }

    private RedisString putRaw(String field, RESPBulkString fieldValue) {
        return putRaw(field,new RedisString(fieldValue));
    }


    public synchronized RedisString get(String field) {
        return value.getOrDefault(field,null);
    }

    public RedisHash() {
        value = new HashMap<>();
    }

    @Override
    public String type() { return "hash"; }
}
