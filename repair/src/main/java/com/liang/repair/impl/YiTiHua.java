package com.liang.repair.impl;

import com.liang.common.database.template.JdbcTemplate;
import com.liang.common.dto.ExecMode;
import com.liang.common.util.GlobalUtils;
import com.liang.repair.trait.AbstractRunner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class YiTiHua extends AbstractRunner {
    @Override
    public void run(String[] args) throws Exception {
        ParameterTool parameterTool = GlobalUtils.getParameterTool();
        JdbcTemplate jdbcTemplate = jdbc(parameterTool.get("db"));
        //List<String> orgIds = jdbcTemplate.queryForList("select distinct org_id from dwd_departments where api_code is not null order by org_id", rs -> rs.getString(1));
        String[] orgIds = "huan,lylb,moka-test-xiunan,simceredx,synapsor,winhealth,infwaveshr,moka-testtemp,plus,wenext,zcool,inke,leiyankeji,okscooter,smzdm,uniin,aftership,doublefs,glzhealth,huanleguang,indeco,moka,qike366,seer,shebao51,ubisor,you,bonree,burgeon,cocos,koolearn,openpie,tec-do,wayz,enjoymi,genhigh,weiling,yaowutech,zdvictory".split(",");
        for (String orgId : orgIds) {
            String sql = "select id,api_code from dwd_departments where api_code is not null and org_id = '" + orgId + "'";
            log.info("query sql: {}", sql);
            List<Tuple2<String, String>> departmentIdAndApiCodes = jdbcTemplate.queryForList(sql, rs -> Tuple2.of(rs.getString(1), rs.getString(2)));
            for (Tuple2<String, String> departmentIdAndApiCode : departmentIdAndApiCodes) {
                String departmentId = departmentIdAndApiCode.f0;
                String apiCode = departmentIdAndApiCode.f1;
                String sql1 = String.format("update dwd_jobs set department_api_code = '%s' where org_id = '%s' and department_id = '%s'", apiCode, orgId, departmentId);
                jdbcTemplate.update(sql1, ExecMode.EXEC);
                String sql2 = String.format("update dwd_headcounts set department_api_code = '%s' where org_id = '%s' and department_id = '%s'", apiCode, orgId, departmentId);
                jdbcTemplate.update(sql2, ExecMode.EXEC);
                TimeUnit.MILLISECONDS.sleep(200);
            }
        }
    }
}
