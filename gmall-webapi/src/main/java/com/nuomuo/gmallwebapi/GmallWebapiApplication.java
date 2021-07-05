package com.nuomuo.gmallwebapi;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.nuomuo.gmallwebapi.mapper")
public class GmallWebapiApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallWebapiApplication.class, args);
    }

}
