package org.tyc.mybatis.mapper.company_legal_person;

import org.apache.ibatis.annotations.Select;
import org.tyc.entity.CompanyLegalPerson;

import java.util.List;

public interface CompanyLegalPersonMapper {
    @Select("select * from company_legal_person where company_id=#{companyId}")
    List<CompanyLegalPerson> findCompanyLegalPerson(Long companyId);


    @Select("select * from company_base.company_legal_person  where legal_rep_human_id=#{hpid}")
    List<CompanyLegalPerson> findCompanyLegalPersonByLegalPerson(String hpid);

    @Select("select * from company_base.company_legal_person  where legal_rep_name_id=#{companyId} ")
    List<CompanyLegalPerson> findCompanyLegalPersonByLegalCompany(Long companyId);
}
