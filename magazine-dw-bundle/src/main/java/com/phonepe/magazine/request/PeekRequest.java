package com.phonepe.magazine.request;

import java.util.Map;
import java.util.Set;

public record PeekRequest(Map<Integer, Set<Long>> pointers) {
}
