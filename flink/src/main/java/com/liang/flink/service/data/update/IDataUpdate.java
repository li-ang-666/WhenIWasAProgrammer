package com.liang.flink.service.data.update;


import com.liang.flink.dto.SingleCanalBinlog;

import java.io.Serializable;
import java.util.List;

public interface IDataUpdate<OUT> extends Serializable {
    List<OUT> updateWithReturn(SingleCanalBinlog singleCanalBinlog);

    List<OUT> deleteWithReturn(SingleCanalBinlog singleCanalBinlog);
}
