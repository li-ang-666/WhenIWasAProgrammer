package com.liang.repair.test;

import cn.hutool.core.util.ReUtil;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.regex.Pattern;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        String str = "[{\"winning_bid_organization\": \"云南天成科技有限公司;云南奥汇成科技有限公司;昆明威豪计算机有限公司\", \"winning_bid_amount_info\": [{\"winning_bid_amount\": \"1：1550元;2：24600元;3：11880元;4：8200元;5：97900元;6：1200元;7：111000元;8：104000元;9：900元;10：17200元;11：2900元;12：115000元;13：272000元;14：14400元;15：18000元;16：17400元;17：43500元;18：125200元;19：56880元;20：21000元;21：33000元;22：23200元;23：11000元;24：3300元;25：71100元\"}]}]";
        System.out.println(ReUtil.findAllGroup0(Pattern.compile("\\d+(\\.\\d+)*万?元"), str));
    }
}
