package com.liang.repair.impl;

import com.liang.common.database.template.JdbcTemplate;
import com.liang.common.dto.ExecMode;
import com.liang.common.util.GlobalUtils;
import com.liang.common.util.SqlUtils;
import com.liang.repair.trait.AbstractRunner;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Ats extends AbstractRunner {
    @Override
    public void run(String[] args) throws Exception {
        ParameterTool parameterTool = GlobalUtils.getParameterTool();
        JdbcTemplate from = jdbc(parameterTool.get("from"));
        JdbcTemplate to = jdbc(parameterTool.get("to"));
        List<Map<String, Object>> maps = from.queryForList("select * from hcm_ats_recruit_record", rs ->
                new HashMap<String, Object>() {{
                    put("id", rs.getString(1));
                    put("recuit_id", rs.getString(2));
                    put("ats_hc_id", rs.getString(3));
                    put("ent_id", rs.getString(4));
                    put("bu_id", rs.getString(5));
                    put("recruitment_mode", rs.getString(6));
                    put("status", rs.getString(7));
                    //put("requset_param", rs.getString(8));
                    //put("response_data", rs.getString(9));
                    put("create_time", rs.getString(10));
                    put("update_time", rs.getString(11));
                    put("creater", rs.getString(12));
                    put("updater", rs.getString(13));
                    put("version", rs.getString(14));
                    //put("fail_msg", rs.getString(15));
                }}
        );
        for (Map<String, Object> map : maps) {
            Object atsHcId = map.get("ats_hc_id");
            String orgId = to.queryForObject("select org_id from dwd_headcounts where id = " + SqlUtils.formatValue(atsHcId), rs -> rs.getString(1));
            map.put("org_id", orgId);
            StringBuilder fields = new StringBuilder();
            StringBuilder values = new StringBuilder();
            map.forEach((k, v) -> {
                fields.append(",").append(k);
                values.append(",").append(SqlUtils.formatValue(v));
            });
            fields.deleteCharAt(0);
            values.deleteCharAt(0);
            String sql = "replace into dwd_hcm_ats_recruit_record(" + fields.toString() + ") values(" + values.toString() + ")";
            to.update(sql, ExecMode.EXEC);
        }
    }
}
