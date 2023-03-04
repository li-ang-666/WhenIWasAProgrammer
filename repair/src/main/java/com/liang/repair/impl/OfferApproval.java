package com.liang.repair.impl;

import com.liang.common.database.template.JdbcTemplate;
import com.liang.common.dto.ExecMode;
import com.liang.common.util.GlobalUtils;
import com.liang.repair.trait.AbstractRunner;
import org.apache.flink.api.java.utils.ParameterTool;

import static com.liang.common.util.SqlUtils.formatValue;

public class OfferApproval extends AbstractRunner {
    @Override
    public void run(String[] args) throws Exception {
        ParameterTool parameterTool = GlobalUtils.getParameterTool();
        JdbcTemplate read = jdbc(parameterTool.get("arg1"));
        JdbcTemplate write = jdbc(parameterTool.get("arg2"));
        read.queryForList("select id,approval_flow_id from offers where deleted = 0 and approval_flow_id is not null limit 9999999", rs -> {
            String offerId = rs.getString(1);
            String flowId = rs.getString(2);
            String sql = String.format("update dwd_offer_approval_flows set business_type = 'OFFER', business_id = %s where id = %s", formatValue(offerId), formatValue(flowId));
            write.update(sql, ExecMode.TEST);
            return null;
        });
    }
}
