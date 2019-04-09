package org.hmf.tsdb;

import java.util.Iterator;
import java.util.LinkedList;

import org.hmf.tsdb.exception.ConnectionFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientFactory {
	static Logger logger = LoggerFactory.getLogger(ClientFactory.class);
	private LinkedList<Client> clients = new LinkedList<Client>();;
	private String host;
	private int port;
	private int maxConnections;
	private int timeout4Response=5000;
	
	public int getMaxConnections() {
		return maxConnections;
	}
	public void setMaxConnections(int maxConnections) {
		this.maxConnections = maxConnections;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	public int getTimeout4Response() {
		return timeout4Response;
	}
	public void setTimeout4Response(int timeout4Response) {
		this.timeout4Response = timeout4Response;
	}
	
	
	public LinkedList<Client> getClients() {
		return clients;
	}
	public void setClients(LinkedList<Client> clients) {
		this.clients = clients;
	}
	
	public  Client getClient() throws ConnectionFailedException{
		Client client = null;		
		
		synchronized(clients) {
			Iterator<Client> iter = clients.iterator();			
			while(iter.hasNext()){
				Client one = iter.next();
				if(one.getHoldBy()!=null) {
					continue;
				}
				
				if(one.isClosed()) {
					iter.remove();
					continue;
				}
				client = one;
			}

			if(client==null && clients.size()<this.maxConnections) {
				try {
					client = Client.getInstance(host, port,this.timeout4Response);
				}catch(Exception e) {
					logger.error("failed to get a new client! "+e.getMessage());
				}
				if(client!=null) {
					clients.add(client);
					logger.info("get a new client,current client amount is "+clients.size());
				}
			}
			
			if(client==null) {
				throw new ConnectionFailedException("can not get a idle client!");
			}else{
				client.setHoldBy(Thread.currentThread());
				logger.debug("hold a tsdb client {} by {}",client,client.getHoldBy());
			}
		}
		
		return client;
	}
	
}
