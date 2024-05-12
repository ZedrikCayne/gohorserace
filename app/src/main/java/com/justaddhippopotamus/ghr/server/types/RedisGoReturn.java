package com.justaddhippopotamus.ghr.server.types;

@FunctionalInterface
public interface RedisGoReturn<T extends RedisType> {
    T accept();
}
