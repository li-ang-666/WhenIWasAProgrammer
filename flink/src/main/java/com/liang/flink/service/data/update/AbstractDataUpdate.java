package com.liang.flink.service.data.update;


import com.liang.flink.dto.SingleCanalBinlog;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDataUpdate<OUT> implements IDataUpdate<OUT> {
    @Override
    public List<OUT> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return new ArrayList<>();
    }

    @Override
    public List<OUT> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return new ArrayList<>();
    }
}
