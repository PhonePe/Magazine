package com.phonepe.growth.magazine.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetaData {
    private long fireCounter;
    private long loadCounter;
}