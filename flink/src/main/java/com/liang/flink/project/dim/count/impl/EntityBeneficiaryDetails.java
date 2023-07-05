package com.liang.flink.project.dim.count.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.dim.count.dao.EntityBeneficiaryDetailsDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.List;

public class EntityBeneficiaryDetails extends AbstractDataUpdate<HbaseOneRow> {
    private final EntityBeneficiaryDetailsDao dao = new EntityBeneficiaryDetailsDao();

    @Override
    public List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return super.updateWithReturn(singleCanalBinlog);
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
