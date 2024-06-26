package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.IRESPFactory;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.RESP.RESPInteger;
import com.justaddhippopotamus.ghr.server.types.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TypeStorage {
    private ConcurrentHashMap<String,RedisType> inMemoryBasic;
    private final String mainStorageFile;
    private final ConcurrentHashMap<Integer,TypeStorage> storages = new ConcurrentHashMap<>();
    private TypeStorage parent = null;
    private int initialSize;

    public int touch(List<String> keys) {
        int count = 0;
        for(var s:keys) if(inMemoryBasic.containsKey(s))++count;
        return count;
    }
    public synchronized int move(String key, int db) {
        final TypeStorage destination = getStorage(db);
        if( destination == this ) {
            return 0;
        }
        RedisType source = inMemoryBasic.getOrDefault(key,null);
        if( source == null ) {
            return 0;
        }
        synchronized (destination) {
            if( destination.keyExists( key ) ) {
                return 0;
            }
            inMemoryBasic.remove(key);
            destination.inMemoryBasic.put(key,source);
        }
        return 1;
    }

    TypeStorage getStorage(int which) {
        if( parent != null ) return parent.getStorage(which);
        if( which == 0 ) return this;
        return storages.computeIfAbsent(which, k -> new TypeStorage(this) );
    }

    //Raw dog store
    public synchronized void store(String key, RedisType value) {
        inMemoryBasic.put(key,value);
    }

    //Store with options. NX = Store value if key doesn't exist.
    //                    PX = Store value only if key exists.
    //                    KEEPTTL = Keep the ttl from the key that is already there...
    //Returns true if error.
    public synchronized boolean store(final String key, RedisType value, final boolean NX, final boolean PX, final boolean KEEPTTL ) {
        if( NX || PX || KEEPTTL ) {
            RedisType rt = inMemoryBasic.getOrDefault(key, null);
            boolean rtValid = !RedisType.isNullOrExpired(rt);
            if( KEEPTTL && rtValid ) value.expireAtMilliseconds = rt.expireAtMilliseconds;
            if( rtValid && NX ) return true;
            if( !rtValid && PX ) return true;
        }
        inMemoryBasic.put(key,value);
        return false;
    }

    public synchronized RedisType storeWithGet(final String key, RedisType value, final boolean KEEPTTL ) {
        RedisType rt = inMemoryBasic.getOrDefault(key, null);
        if( KEEPTTL && !RedisType.isNullOrExpired(rt) )
            value.expireAtMilliseconds = rt.expireAtMilliseconds;
        inMemoryBasic.put(key,value);
        return rt;
    }
    public synchronized <T extends RedisType> T storeWithGetClassed(final String key, T value, Class<T> type, final boolean KEEPTTL ) {
        T oldType = fetchRW(key, type);
        if( KEEPTTL && oldType != null )
            value.expireAtMilliseconds = oldType.expireAtMilliseconds;
        inMemoryBasic.put(key,value);
        return oldType;
    }

    public synchronized boolean keyExists(final String key) {
        RedisType rawFetch = inMemoryBasic.getOrDefault(key, null);
        return RedisType.exists(rawFetch);
    }

    public synchronized boolean del(final String key) {
        RedisType rt = fetchDelOnExpire(key);
        if( rt != null ) {
            inMemoryBasic.remove(key);
            return true;
        }
        return false;
    }

    public String randkey() {
        Set<String> s = getKeySetCopy();
        int len = s.size();
        if( len == 0 ) return null;
        if( len == 1 ) return s.stream().findFirst().get();
        Iterator<String> i = s.stream().iterator();
        Random r = new Random();
        int target = r.nextInt(s.size()-1);
        try {
            String returnValue = i.next();
            for( int j = 0; j < target; ++j ) {
                returnValue = i.next();
            }
            return returnValue;
        } catch (Exception e) {
            return null;
        }
    }

    public synchronized RedisType fetch(final String key) {
        return fetchDelOnExpire(key);
    }

    private synchronized RedisType fetchDelOnExpire(final String key) {
        RedisType rt = inMemoryBasic.getOrDefault(key, null);
        if( rt != null ) {
            if( rt.expired() || rt.canBeReaped() ) {
                inMemoryBasic.remove(key);
                return null;
            }
        }
        return rt;
    }

    public synchronized <T extends RedisType> T fetchRW(final String key, final Class<T> whichType ) {
        RedisType rawFetch = fetchDelOnExpire(key);
        if( rawFetch == null ) {
            return null;
        }
        if( whichType.isInstance(rawFetch) ) {
            return (T)rawFetch;
        }
        throw new RuntimeException("WRONGTYPE: Wanted " + whichType.getSimpleName() + " but got " + rawFetch.getClass().getSimpleName() );
    }
    public synchronized final <T extends RedisType> T fetchRO(String key, Class<T> whichType ) {
        return fetchRW(key,whichType);
    }

    public synchronized final <T extends RedisType> List<T> fetchROMany(List<String> keys, Class<T> whichType ) {
        List<T> returnType = new ArrayList<>(keys.size());
        for( String key : keys ) {
            returnType.add( fetchRO(key, whichType) );
        }
        return returnType;
    }
    public synchronized <T extends RedisType> List<T> fetchRWMany(List<String> keys, Class<T> whichType, RedisGoReturn<T> f ) {
        List<T> returnType = new ArrayList<>(keys.size());
        for( String key : keys ) {
            returnType.add( fetchRW(key,whichType,f) );
        }
        return returnType;
    }
    public synchronized <T extends RedisType> T fetchRW(String key, Class<T> whichType, RedisGoReturn<T> f) {
        T rawFetch = fetchRW(key,whichType);
        if( rawFetch == null ) {
            T defaultValue = f.accept();
            inMemoryBasic.put(key,defaultValue);
            return defaultValue;
        }
        return rawFetch;
    }

    public synchronized <T extends RedisType> T fetchDel(String key, Class<T> whichType) {
        T rawFetch = fetchRW(key,whichType);
        if( rawFetch == null )
            return null;
        if( !whichType.isInstance(rawFetch) )
            return null;
        inMemoryBasic.remove(key);
        return rawFetch;
    }
    public void shutDown() {
        writeFile();
    }
    public void writeDb() {
        writeFile();
    }

    private static void dbLoad(TypeStorage toLoad, IRESPFactory respFactory, RedisTypeFactory typeFactory, InputStream fis ) throws IOException {
        toLoad.inMemoryBasic = new ConcurrentHashMap<>();
        toLoad.initialSize = respFactory.getNextRESPInteger(fis);
        toLoad.inMemoryBasic = new ConcurrentHashMap<>(toLoad.initialSize);
        for( int i = 0; i < toLoad.initialSize; ++i ) {
            RESPBulkString bs = (RESPBulkString)respFactory.getNext(fis);
            if( bs == null ) {
                throw new RuntimeException("Was expecting a Bulk String at element " + i );
            }
            RedisType rt = typeFactory.from(fis);
            if( rt == null ) {
                throw new RuntimeException("Failed to grab a proper redis type from the stream at element " + i);
            }
            toLoad.inMemoryBasic.put(bs.toString(),rt);
        }
    }

    private void loadFile() {
        if( mainStorageFile == null ) {
            LOG.warn("DB not read, running in memory only mode.");
            return;
        }
        try {
            FileInputStream fis = new FileInputStream(mainStorageFile);
            //Read version number off the front
            byte[] header = fis.readNBytes(6);
            if( header[ 0 ] != 'R' && header[ 1 ] != 'E' && header[ 2 ] != 'S' &&
                header[ 3 ] != 'P' && header[ 4 ] != 13 && header[ 5 ] != 10 )
                throw new RuntimeException("DB not a RESP file.");
            IRESPFactory respFactory = IRESPFactory.getDefault();
            RedisTypeFactory typeFactory = RedisTypeFactory.getDefault();
            //Load the first database.
            dbLoad(this,respFactory,typeFactory,fis);
            while( fis.available() > 0 ) {
                int newDbIndex = respFactory.getNextRESPInteger(fis);
                TypeStorage newDb = getStorage(newDbIndex);
                dbLoad( newDb, respFactory, typeFactory, fis);
            }
            fis.close();
        } catch (FileNotFoundException e) {
            //File not found on startup, we're probably ok?
            LOG.warn(mainStorageFile + " not found...new startup.");
            inMemoryBasic = new ConcurrentHashMap<>(initialSize);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load main DB.", e);
        }
        LOG.info("Done reading db");
    }

    private void writeTo(OutputStream fos) throws IOException {
        RESPInteger in = new RESPInteger(inMemoryBasic.size());
        in.publishTo(fos);
        for( String key : inMemoryBasic.keySet() ) {
            RESPBulkString bs = new RESPBulkString(key);
            bs.publishTo(fos);
            inMemoryBasic.get(key).writeTo(fos);
        }
    }

    private void writeFile() {
        if( mainStorageFile == null ) {
            LOG.info("Not saving the DB. Memory only requested.");
            return;
        }
        LOG.log("Writing db.");
        try {
            FileOutputStream fos = new FileOutputStream(mainStorageFile);
            fos.write( 'R' ); fos.write( 'E' ); fos.write( 'S' );
            fos.write( 'P' ); fos.write( 13 ); fos.write( 10 );
            writeTo(fos);
            for( Integer i : storages.keySet() ) {
                new RESPInteger(i).publishTo(fos);
                storages.get(i).writeTo(fos);
            }

            fos.close();
        } catch(FileNotFoundException e) {
            LOG.error("Could not write to the output db..about to lose some stuff.");
        } catch(IOException e) {
            LOG.error("Could not write to the output db..about to lose some stuff.");
        }
        LOG.log("Done writing file.");
    }
    public TypeStorage(String mainStorageFile, int initialSize) {
        this.initialSize = initialSize;
        this.mainStorageFile = mainStorageFile;
        loadFile();
    }

    public TypeStorage(TypeStorage parent) {
        this.mainStorageFile = null;
        this.initialSize = 1024;
        this.parent = parent;
        this.inMemoryBasic = new ConcurrentHashMap<>(1024);
    }

    private synchronized Set<String> getKeySetCopy() {
        return new HashSet<>(inMemoryBasic.keySet());
    }

    public Set<String> keys(String glob) {
        Set<String> newKeys = getKeySetCopy();
        return Utils.getMatches(newKeys,glob);
    }

    public Set<String> keys() {
        return getKeySetCopy();
    }

    public static long MSETNX_SET_NONE = 0;
    public static long MSETNX_SET_ALL = 1;
    public long mset(Map<String,RedisString> toSet, boolean NX) {
        Set<String> keySet = toSet.keySet();
        if( NX ) {
            for( String key : keySet ) {
                if( fetchDelOnExpire(key) != null ) {
                    return MSETNX_SET_NONE;
                }
            }
        }
        for( String key : toSet.keySet() ) {
            inMemoryBasic.put(key,toSet.get(key));
        }
        return MSETNX_SET_ALL;
    }

    public synchronized List<RedisType> mget(Collection<String> keys) {
        return keys.stream().map(this::fetchDelOnExpire).collect(Collectors.toList());
    }

    public synchronized List<RedisString> mgetString(Collection<String> keys) {
        return keys.stream().map( key -> this.fetchRW(key,RedisString.class) ).collect(Collectors.toList());
    }

    public static final long RENAME_FAIL_ALREADY_EXIST = 0;
    public static final long RENAME_SUCCESS = 1;
    public static final long RENAME_FAIL_NO_SOURCE = -1;
    public synchronized long rename(String source, String destination, boolean NX) {
        RedisType rt = fetchDelOnExpire(source);
        if( rt == null ) {
            return RENAME_FAIL_NO_SOURCE;
        }
        if( NX ) {
            if( fetchDelOnExpire(destination) != null )
                return RENAME_FAIL_ALREADY_EXIST;
        }
        inMemoryBasic.remove(source);
        inMemoryBasic.put( destination, rt );
        return RENAME_SUCCESS;
    }

    public synchronized void flushAll() {
        if( parent != null ) parent.flushAll();
        else {
            flush();
            for(var v : storages.values()) v.flush();
        }
    }
    public synchronized void flush() {
        inMemoryBasic = new ConcurrentHashMap<>();
    }

    public synchronized void swapdb(int a, int b) {
        final TypeStorage ta = getStorage(a);
        final TypeStorage tb = getStorage(b);
        synchronized (tb) {
            var temp = ta.inMemoryBasic;
            ta.inMemoryBasic = tb.inMemoryBasic;
            tb.inMemoryBasic = temp;
        }
    }

    public synchronized int size() {
        return inMemoryBasic.size();
    }

    private static final Logger LOG = Logger.get(TypeStorage.class.getSimpleName());
}
