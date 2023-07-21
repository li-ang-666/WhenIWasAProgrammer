package org.tyc.mybatis;

import org.tyc.mybatis.mapper.share_holder_label.CompanyHumanPositionMapper;
import org.tyc.mybatis.runner.JDBCRunner;


public class MybatisTest {
    public static void main(String[] args) {
//        TestMapper mapper = JDBCRunner.getMapper(TestMapper.class, "9349c027b3b4414aa5f9019cd218e7a3in01/test_online.xml");
//        System.out.println(mapper.queryTest(1L));

        for (int i = 0; i < 100; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        CompanyHumanPositionMapper mapper = JDBCRunner.getMapper(CompanyHumanPositionMapper.class, "e1d4c0a1d8d1456ba4b461ab8b9f293din01/prism_shareholder_path.xml");
                        String s = mapper.queryPositions("", "2157371442");
                        System.out.println(s);
                    }


                }
            });
            thread.setName("djfdf+1");
            thread.start();

        }


    }
}