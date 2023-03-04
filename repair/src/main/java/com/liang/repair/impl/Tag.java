package com.liang.repair.impl;

import com.liang.common.database.template.JdbcTemplate;
import com.liang.common.util.JsonUtils;
import com.liang.repair.trait.AbstractRunner;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;

@Slf4j
public class Tag extends AbstractRunner {
    @Override
    public void run(String[] args) throws Exception {
        JdbcTemplate jdbc = jdbc("dorisproduction2");
        jdbc.queryForList("select a.org_id,b.name,group_concat(a.content,'#####'),group_concat(concat(a.type,' : ',a.content)) \n" +
                "from dwd_config_variables a join dwd_organizations b \n" +
                "on a.org_id = b.id and b.pay = 1 and b.expired_at >= now() \n" +
                "where a.type in ('builtin_intelligent_tag_setting','builtin_ai_tag_setting') \n" +
                "group by a.org_id,b.name \n" +
                "order by a.org_id", rs -> {
            ArrayList<Integer> t = new ArrayList<>();
            ArrayList<Integer> f = new ArrayList<>();
            String orgId = String.valueOf(rs.getString(1));
            String orgName = String.valueOf(rs.getString(2));
            String content = String.valueOf(rs.getString(3));
            String detail = String.valueOf(rs.getString(4));
            HashMap<String, Object> jsonMap = new HashMap<>();
            for (String s : content.split("#####")) {
                jsonMap.putAll(JsonUtils.parseJsonObj(s));
            }
            jsonMap.forEach((k, v) -> {
                if ("true".equals(String.valueOf(v)))
                    t.add(1);
                else if ("false".equals(String.valueOf(v)))
                    f.add(1);
                else
                    log.error(orgId + "有问题");
            });
            System.out.println(format(20, orgId)
                    + format(40, orgName)
                    + format(5, String.valueOf(t.size()))
                    + format(5, String.valueOf(f.size()))
                    + detail);
            return null;
        });
    }

    public String format(int length, String value) {
        /*int k = length - value.length();
        StringBuilder builder = new StringBuilder(value);
        for (int i = 0; i < k; i++) {
            builder.append(" ");
        }
        return builder.toString();*/
        return value + "\t";
    }
}
