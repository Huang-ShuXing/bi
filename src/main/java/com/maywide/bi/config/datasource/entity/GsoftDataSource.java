package com.maywide.bi.config.datasource.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


@Entity
@Table(name = "BI_DATA_SOURCE")
public class GsoftDataSource {
    @Id
//    @SequenceGenerator(name = "IA_DATA_SOURCE_SEQ", sequenceName = "BI_DATA_SOURCE_SEQ", allocationSize = 1)
//    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "BI_DATA_SOURCE_SEQ")
    private Long id;

    /**
     * data source name
     */
    @Column(name = "C_NAME", unique = true)
    private String name;

    /**
     * data source type, default is database<br />
     */
    @Column(name = "C_TYPE")
    private Integer type = DataSourceType.DB.intValue();

    /**
     * 数据库类型，目前只支持MySql和Oracle<br />
     */
    @Column(name = "C_DATA_TYPE")
    private Integer dataType = DataType.ORACLE.intValue();

    @Column(name = "C_URL")
    private String url;

    @Column(name = "C_USER_NAME")
    private String userName;

    @Column(name = "C_PASSWORD")
    private String password;

    @Column(name = "C_JNDI_NAME")
    private String jndiName;

    @Column(name = "C_DRIVER_CLASS_NAME")
    private String driverClassName;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getJndiName() {
        return jndiName;
    }

    public void setJndiName(String jndiName) {
        this.jndiName = jndiName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public Integer getDataType() {
        return dataType;
    }

    public void setDataType(Integer dataType) {
        this.dataType = dataType;
    }
    public enum DataType {
        ORACLE(0),
        MYSQL(1);
        private Integer value;
        public Integer intValue() {
            return this.value;
        }
        DataType(Integer value) {
            this.value = value;
        }
    }
    public enum DataSourceType {
        DB(0),
        ss(1);
        private Integer value;
        DataSourceType(Integer value) {
            this.value = value;
        }

        public Integer intValue() {
            return this.value;
        }
    }
}