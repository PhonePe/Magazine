package com.phonepe.magazine.response;

import java.util.List;

public record PeekResponse(String identifier, List<PeekedData> data) {
}
