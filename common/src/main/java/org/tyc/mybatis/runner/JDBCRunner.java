package org.tyc.mybatis.runner;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.session.SqlSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tyc.mybatis.config.LogUtil;
import org.tyc.mybatis.mapper.general.GeneralMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wangshiwei
 * @date 2023/1/31 9:35 上午
 * @description 新版本查询数据库方法，此版本暂无事务相关
 **/
public class JDBCRunner<T> implements AutoCloseable, Serializable {
    private static Logger log = LoggerFactory.getLogger(Thread.currentThread().getStackTrace()[1].getClassName());
    private static String basePath = "mybatis/instance/%s.xml";
    private SqlSession sqlSession;
    private String instance;
    private Class<T> clazz;


    private JDBCRunner(Class<T> clazz, String instance) {
        this.clazz = clazz;
        this.instance = instance;
    }

    /**
     * 获取 JDBCRunner 请使用本类中getMapper方法
     *
     * @param clazz
     * @param instance
     * @param <T>
     * @return
     */
    @Deprecated
    public static <T> JDBCRunner<T> getRunner(Class<T> clazz, String instance) {
        return new JDBCRunner<T>(clazz, instance);
    }

    /**
     * 获取通用的查询mapper.
     *
     * @param instance
     * @return
     */
    public static GeneralMapper getGeneralMapper(String instance) {
        return getMapper(GeneralMapper.class, instance);
    }

    public static <T> T getMapper(Class<T> clazz, String instance) {
        // 兼容schema后面有.xml
        instance = instance.replace(".xml", "");

        JDBCRunner<T> jdbcRunner = new JDBCRunner<>(clazz, instance);
        SqlSessionManager sqlSessionManager = DbUtil.getSQLSessionManager(clazz, jdbcRunner.instance);
        T mapper = sqlSessionManager.getMapper(clazz);
        return mapper;
    }


    /**
     * 获取mapper 默认autoCommit.
     *
     * @return
     */

    @Deprecated
    public T getMapper() {
        SqlSessionManager sqlSessionManager = DbUtil.getSQLSessionManager(clazz, this.instance);
        T mapper = sqlSessionManager.getMapper(clazz);
        return mapper;
    }

    /**
     * 关闭sqlSession.
     */
    @Override
    public void close() {
        if (sqlSession != null) {
            sqlSession.close();
        }
    }

    private static class DbUtil {

        private final static Map<String, SqlSessionManager> map = new ConcurrentHashMap<>();
        private static Logger log = LoggerFactory.getLogger(Thread.currentThread().getStackTrace()[1].getClassName());


        public static SqlSessionManager getSQLSessionManager(Class clazz, String instance) {

            if (isEmpty(instance)) {
                throw new RuntimeException("DbUtil instance is empty!!!");
            }
            SqlSessionManager sqlSessionManager;

            if (!map.containsKey(instance)) {
                synchronized (DbUtil.class) {
                    // log.info("初始化连接池线程等待");
                    if (!map.containsKey(instance)) {
                        sqlSessionManager = initDb(instance);

                        map.put(instance, sqlSessionManager);
                        log.info("初始化连接池成功 instance:{}", instance);

                        try {
                            if (!sqlSessionManager.getConfiguration().hasMapper(clazz)) {
                                sqlSessionManager.getConfiguration().addMapper(clazz);
                                log.info("添加新mapper成功 instance:{} ,mapper:{}", instance, clazz);
                            }
                        } catch (Exception e) {
                            log.error("添加新mapper失败 instance:{} ,mapper:{},exception:{}", instance, clazz, e);
                        }
                        return sqlSessionManager;
                    }
                }
            }

            // 到这里肯定能获得
            SqlSessionManager sessionManager = map.get(instance);
            if (sessionManager == null || sessionManager.getConfiguration() == null) {
                log.error("sessionManager is null!! instance:{}", instance);
                throw new RuntimeException("sessionFactory is not null!! please check instance and more");
            }

            if (!sessionManager.getConfiguration().hasMapper(clazz)) {
                // log.info("添加mapper线程等待")
                synchronized (DbUtil.class) {
                    if (!sessionManager.getConfiguration().hasMapper(clazz)) {
                        try {
                            sessionManager.getConfiguration().addMapper(clazz);
                            log.info("添加新mapper成功 instance:{} ,mapper:{}", instance, clazz);
                        } catch (Exception e) {
                            log.error("添加新mapper失败 instance:{} ,mapper:{},exception:{}", instance, clazz, e);
                        }
                    }

                }

            }

            return sessionManager;

        }


        /**
         * 初始化新的SqlSessionManager.
         *
         * @param instance
         * @return
         */
        private static SqlSessionManager initDb(String instance) {
            InputStream in = null;
            try {

                String path = String.format(basePath, instance);
                in = Resources.getResourceAsStream(path);
                SqlSessionFactory sqlSessionFactory;
                if ("default".equals(instance)) {
                    sqlSessionFactory = new SqlSessionFactoryBuilder().build(in);
                    log.warn("db is empty use default environment please check <environments default=\"xxxxxxxxxx\"> from mybatis-config.xml  ");
                } else {
                    Properties properties = new Properties();
                    String[] split = instance.split("/");
                    properties.setProperty("schema", split[1]);
                    sqlSessionFactory = new SqlSessionFactoryBuilder().build(in, "jdbc_mysql", properties);

                }
                return SqlSessionManager.newInstance(sqlSessionFactory);
            } catch (IOException e) {
                throw new RuntimeException("initDb error please check path and config!!! {}", e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        log.error(LogUtil.toStackTrace(e));
                    }
                }
            }
        }

        /**
         * 判断字符串是否为空
         *
         * @param str
         * @return
         */
        private static boolean isEmpty(String str) {

            if (str == null || "".equals(str)) {
                return true;
            }
            return false;
        }


    }
}



