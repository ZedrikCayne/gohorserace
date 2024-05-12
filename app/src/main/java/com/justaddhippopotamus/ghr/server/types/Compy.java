package com.justaddhippopotamus.ghr.server.types;

@FunctionalInterface
public interface Compy<T> {
    boolean accept(T a, T b);
}
