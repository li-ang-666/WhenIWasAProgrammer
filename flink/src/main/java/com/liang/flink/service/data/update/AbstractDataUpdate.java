package com.liang.flink.service.data.update;


import com.liang.common.dto.HbaseOneRow;
import com.liang.flink.dto.SingleCanalBinlog;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDataUpdate implements IDataUpdate {
    @Override
    public List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return new ArrayList<>();
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return new ArrayList<>();
    }
}
