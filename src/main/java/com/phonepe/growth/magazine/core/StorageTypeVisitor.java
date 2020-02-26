package com.phonepe.growth.magazine.core;

/**
 * @author shantanu.tiwari
 */
public interface StorageTypeVisitor<T> {
    T visitAerospike() throws Exception;

    T visitHBase() throws Exception;
}
