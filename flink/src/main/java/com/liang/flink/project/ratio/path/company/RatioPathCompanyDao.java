package com.liang.flink.project.ratio.path.company;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.liang.common.util.SqlUtils.formatValue;

public class RatioPathCompanyDao {
    private final static String SINK_RATIO_PATH_COMPANY = "ratio_path_company";
    private final static String SINK_ENTITY_BENEFICIARY_DETAILS = "entity_beneficiary_details";
    private final static String SINK_ENTITY_CONTROLLER_DETAILS = "entity_controller_details";
    private final static String SINK_SHAREHOLDER_IDENTITY_TYPE_DETAILS = "shareholder_identity_type_details";

    private final JdbcTemplate prismShareholderPath = new JdbcTemplate("457.prism_shareholder_path");
    private final JdbcTemplate bdpEquity = new JdbcTemplate("463.bdp_equity");
    private final JdbcTemplate companyBase465 = new JdbcTemplate("465.company_base");
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");
    private final JdbcTemplate humanBase = new JdbcTemplate("040.human_base");
    private final JdbcTemplate jdbcTemplate157 = new JdbcTemplate("157.listed_base");

    public void deleteAll(Long companyId) {
        //删除ratio_path_company
        String sql0 = new SQL().DELETE_FROM(SINK_RATIO_PATH_COMPANY)
                .WHERE("company_id = " + formatValue(companyId))
                .toString();
        //删除受益人
        String sql1 = new SQL()
                .DELETE_FROM(SINK_ENTITY_BENEFICIARY_DETAILS)
                .WHERE("tyc_unique_entity_id = " + formatValue(companyId))
                .toString();
        //删除控制人
        String sql2 = new SQL()
                .DELETE_FROM(SINK_ENTITY_CONTROLLER_DETAILS)
                .WHERE("company_id_controlled = " + formatValue(companyId))
                .toString();
        //删除身份关系
        String sql3 = new SQL()
                .DELETE_FROM(SINK_SHAREHOLDER_IDENTITY_TYPE_DETAILS)
                .WHERE("tyc_unique_entity_id = " + formatValue(companyId))
                .toString();
        prismShareholderPath.update(sql0);
        bdpEquity.update(sql1, sql2, sql3);
    }

    public boolean isPartnership(Object companyId) {
        String sql = new SQL().SELECT("1")
                .FROM("tyc_entity_general_property_reference")
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(companyId))
                .WHERE("entity_property in (15,16)")
                .toString();
        String res = companyBase465.queryForObject(sql, rs -> rs.getString(1));
        return res != null;
    }

    public boolean isListed(Object companyId) {
        String sql = new SQL().SELECT("1")
                .FROM("company_bond_plates")
                .WHERE("company_id is not null and company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("listed_status is not null and listed_status not in (0, 3, 5, 8, 9)")
                .toString();
        String res = jdbcTemplate157.queryForObject(sql, rs -> rs.getString(1));
        return res != null;
    }

    public void replaceIntoRatioPathCompany(Map<String, Object> columnMap) {
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
        String sql = new SQL().INSERT_INTO(SINK_RATIO_PATH_COMPANY)
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
        prismShareholderPath.update(sql);
    }

    public String getEntityName(String entityId) {
        if (TycUtils.isUnsignedId(entityId)) {
            String sql = new SQL()
                    .SELECT("company_name")
                    .FROM("company_index")
                    .WHERE("company_id = " + formatValue(entityId))
                    .toString();
            String res = companyBase435.queryForObject(sql, rs -> rs.getString(1));
            return res != null ? res : "";
        }
        String sql = new SQL()
                .SELECT("human_name")
                .FROM("human")
                .WHERE("human_id = " + formatValue(entityId))
                .toString();
        String res = humanBase.queryForObject(sql, rs -> rs.getString(1));
        return res != null ? res : "";
    }

    public String replaceIntoBeneficiary(Map<String, Object> columnMap) {
        return replaceInto(SINK_ENTITY_BENEFICIARY_DETAILS, columnMap);
    }

    public String replaceIntoController(Map<String, Object> columnMap) {
        return replaceInto(SINK_ENTITY_CONTROLLER_DETAILS, columnMap);
    }

    private String replaceInto(String tableName, Map<String, Object> columnMap) {
        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
        return new SQL().INSERT_INTO(tableName)
                .INTO_COLUMNS(insert.f0)
                .INTO_VALUES(insert.f1)
                .toString();
    }

    public List<String> replaceIntoShareholderTypeDetail(List<Map<String, Object>> columnMaps) {
        List<String> sqls = new ArrayList<>();
        for (Map<String, Object> columnMap : columnMaps) {
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            String sql = new SQL()
                    .INSERT_INTO(SINK_SHAREHOLDER_IDENTITY_TYPE_DETAILS)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sqls.add(sql);
        }
        return sqls;
    }

    public void updateAll(List<String> sqls) {
        bdpEquity.update(sqls);
    }
}
