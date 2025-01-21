package com.phonepe.magazine.core;

import com.github.rholder.retry.RetryException;

import java.util.concurrent.ExecutionException;

/**
 * @author shantanu.tiwari
 */
public interface StorageTypeVisitor<T> {
    T visitAerospike() throws ExecutionException, RetryException;

    T visitHBase() throws ExecutionException, RetryException;
}
