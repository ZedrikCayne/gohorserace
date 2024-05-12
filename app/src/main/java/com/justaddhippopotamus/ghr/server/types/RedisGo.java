package com.justaddhippopotamus.ghr.server.types;

@FunctionalInterface
public interface RedisGo<T extends RedisType> {
    void accept(T t);
}

