package org.tyc.entity;

import lombok.Data;

/**
 * @author wangshiwei
 * @date 2023/5/17 10:29
 * @description
 **/
@Data
public class HumanLink {
    private String humanName = "";
    private Long companyId = 0L;
    private Long humanNameId = 0L;
    private String humanId = "";
}
