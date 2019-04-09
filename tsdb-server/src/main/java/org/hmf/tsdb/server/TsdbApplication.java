package org.hmf.tsdb.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 
 * @author hexiaodong
 */
@EnableScheduling
@SpringBootApplication(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
public class TsdbApplication {
	public static ApplicationContext context = null;
	public static void main(String[] args) {
		 context = SpringApplication.run(TsdbApplication.class, args);		 
	}
	public ApplicationContext getContext(){
		return context;
	}
	
	public static Object getBean(String name){
		return context.getBean(name);
	}
	
	public static Object getBean(Class<?> clazz){
		return context.getBean(clazz);
	}
	public static void destory() {
		try {
			((ConfigurableApplicationContext )context).close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
