package com.phonepe.growth.magazine.core;

import java.util.Optional;

public interface IMagazine {
    public boolean prepare(String keyPrefix);

    public boolean load(String keyPrefix, Object data);

    public Optional<Object> fire(String keyPrefix);
}
