package com.maywide.bi.config.datasource.dynamic;

public interface Constants {
    public static String DEFAULT_DATA_SOURCE_NAME="dataSource";
    public static String DataSourceType="";


    interface SCHEDULE{
        public static final String STATUS_NORMAL ="1";
        public static final String STATUS_RUNNING ="2";
    }

    interface SCHEDULE_JOB{
        public static final int STATUS_NORMAL = 1;
        public static final int STATUS_NO_USE = 0;
    }
}
