package com.maywide.bi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//注册动态多数据源
//@Import({DynamicDataSource.class})
public class BiApplication {

	public static void main(String[] args) {
		SpringApplication.run(BiApplication.class, args);

	}
}
