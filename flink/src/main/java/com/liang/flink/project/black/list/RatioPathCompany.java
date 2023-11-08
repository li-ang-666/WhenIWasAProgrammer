package com.liang.flink.project.black.list;

import com.liang.common.service.SQL;
import lombok.experimental.UtilityClass;

import java.util.Collections;
import java.util.List;

@UtilityClass
public class RatioPathCompany {
    public static List<String> get() {
        String sql = new SQL().DELETE_FROM("ratio_path_company")
                .WHERE("company_id = 2338203553")
                .toString();
        return Collections.singletonList(sql);
    }
}
