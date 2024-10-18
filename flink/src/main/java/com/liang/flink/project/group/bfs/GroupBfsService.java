package com.liang.flink.project.group.bfs;

import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class GroupBfsService {
    private final GroupBfsDao dao = new GroupBfsDao();

    public void bfs(String companyId) {
        // 格式合法
        if (!TycUtils.isUnsignedId(companyId)) {
            return;
        }
        // 在company_index存在
        Map<String, Object> companyIndexColumnMap = dao.queryCompanyIndex(companyId);
        if (companyIndexColumnMap.isEmpty()) {
            return;
        }
        // 没有股东
        if (dao.queryHasShareholder(companyId)) {
            return;
        }
    }
}
