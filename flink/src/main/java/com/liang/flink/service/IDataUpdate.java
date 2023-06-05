package com.liang.flink.service;


import com.liang.common.dto.HbaseOneRow;
import com.liang.flink.dto.SingleCanalBinlog;

import java.io.Serializable;
import java.util.List;

public interface IDataUpdate extends Serializable {
    List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog);

    List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog);
}
