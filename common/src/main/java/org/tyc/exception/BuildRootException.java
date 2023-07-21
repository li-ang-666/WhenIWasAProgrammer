package org.tyc.exception;

/**
 * @author wangshiwei
 * @date 2023/5/11 17:30
 * @description
 **/
public class BuildRootException extends Exception {
    private Long companyId;

    public BuildRootException(Long companyId) {
        this.companyId = companyId;
    }

    @Override
    public String getMessage() {
        return "该【" + companyId + "】无投资关系";
    }

    @Override
    public synchronized Throwable getCause() {
        return super.getCause();
    }
}
