package com.justaddhippopotamus.ghr.server;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

public class LuaStorage {
    final Map<String,String> luaScriptStorage;

    public LuaStorage() {
        luaScriptStorage = new HashMap<>();
    }

    public synchronized List<Integer> exists(List<String> sha1) {
        List<Integer> returnValue = new ArrayList<>(sha1.size());
        return sha1.stream().map(x->luaScriptStorage.containsKey(x)?1:0).collect(Collectors.toList());
    }

    public synchronized String load(String luaCode) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            String sha1 = Base64.getEncoder().encodeToString(md.digest(luaCode.getBytes(Server.CHARSET)));
            luaScriptStorage.put(sha1,luaCode);
            return sha1;
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    public synchronized void flush(boolean async) {
        luaScriptStorage.clear();
    }

    public synchronized String getBySHA1(String sha1) {
        return luaScriptStorage.getOrDefault(sha1,null);
    }
}
