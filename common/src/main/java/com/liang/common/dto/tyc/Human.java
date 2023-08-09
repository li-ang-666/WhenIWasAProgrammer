package com.liang.common.dto.tyc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Human {
    @NonNull
    private Long gid = 0L;
    @NonNull
    private String name = "";
}
