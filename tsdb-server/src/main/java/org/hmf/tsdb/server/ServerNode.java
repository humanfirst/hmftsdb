package org.hmf.tsdb.server;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ServerNode{	
	@Value("${tsdb.service.url:127.0.0.1:7677}")
	private String url;
	
	private String serviceHost;
	private Integer servicePort;
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}


	private void parseUrl() {
		String[] ss = this.url.split(":");
		this.serviceHost = ss[0];
		this.servicePort = Integer.valueOf(ss[1]);
	}
	
	public String getServiceHost() {
		if(serviceHost==null) {
			this.parseUrl();
		}
		return serviceHost;
	}
	
	public Integer getServicePort() {
		if(servicePort==null) {
			this.parseUrl();
		}
		return this.servicePort;
	}
	
}
