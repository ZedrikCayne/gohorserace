package com.justaddhippopotamus.ghr.server.types;

import java.util.List;

@FunctionalInterface
public interface RedisGoMany<T,R> {
    R accept(List<T> allOf);
}
