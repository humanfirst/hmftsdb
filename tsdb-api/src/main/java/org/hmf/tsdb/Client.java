package org.hmf.tsdb;


import java.util.ArrayList;
import java.util.List;

import org.hmf.tsdb.Request.Method;
import org.hmf.tsdb.exception.ConnectClosedByPeer;
import org.hmf.tsdb.exception.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;

public class Client extends ChannelInboundHandlerAdapter{
	private static final Logger logger = LoggerFactory.getLogger(Client.class);	
	private String response ;
	private Object holdBy = null;	
	
	private ObjectMapper objMapper = null;
	private String host;
	private int port;
	private Channel channel;
	
	
	public enum State{
		send, //发送状态
		idle, //空闲状态
		wait, //等待服务器响应状态
		receive; //接收状态
	}
	
	private State state = State.idle;
	
	private int timeout4Response = -1;
	
	public static Client getInstance(String host,int port,int timeout4Response) throws Exception{
		Client client = build(host,port);		
		client.timeout4Response = timeout4Response;
		return client;
		
	}
	

	public Object getHoldBy() {
		return holdBy;
	}


	public void setHoldBy(Object holdBy) {
		this.holdBy = holdBy;
	}
	
	public boolean isClosed() {
		return channel==null;
	}



	public void release() {
		this.holdBy = null;
	}

	
	private static Client build(String host,int port)throws Exception{
		final Client client = new Client();
		client.host = host;
		client.port = port;
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(workerGroup);
		bootstrap.channel(NioSocketChannel.class);
		bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(new LineBasedFrameDecoder(100*1024*1024));
            	ch.pipeline().addLast(new StringDecoder());
            	ch.pipeline().addLast(client);
			}
		});

		try {
			client.channel = bootstrap.connect(host, port).sync().channel();				
			return client;
		} catch (Exception e) {	
			if(workerGroup!=null) {
				workerGroup.shutdownGracefully();
			}
			throw new Exception("failed to connect tsdb server host="+host+",port="+port,e);
		}finally {
			
		}
	}
	
	List<DataPoint> shouldPutDps = new ArrayList<DataPoint>();
	
	protected Client() {
		objMapper = new ObjectMapper();
	}
	
	public State getState() {
		return state;
	}
	public void setState(State state) {
		this.state = state;
	}
	@Override    
    public void channelActive(ChannelHandlerContext ctx) throws Exception { 
		logger.info("connected tsdb server host={} ,port={}",host,port);		
	}
	
	
	public void putDpsInStr(String dps) throws TimeoutException, ConnectClosedByPeer {
		StringBuffer sb = new StringBuffer();
		sb.append("{\"method\":\"put\",\"data\":")
			.append(dps).append("}");
		this.put(sb.toString());
	}
	
	public void put(List<DataPoint> dps){
		if(dps!=null){
			this.shouldPutDps.addAll(dps);	
		}
	}

	public void put(DataPoint dp){
		if(dp!=null){
			this.shouldPutDps.add(dp);
		}
	}
	
	public void flush() throws TimeoutException, ConnectClosedByPeer {
		doPut(shouldPutDps);
		this.shouldPutDps.clear();
	}
	
	private void doPut(List<DataPoint> points) throws TimeoutException, ConnectClosedByPeer{
		String msg = null;
		Request req = new Request();
		req.setGroupIdx(0);
		req.setData(points);
		req.setMethod(Method.put);
		try{
			msg = objMapper.writeValueAsString(req);		
		}catch(Exception e){
			logger.error("failed to convert dps to String !",e);
		}
		this.put(msg);
	}
	
	public String query(String query) throws TimeoutException, ConnectClosedByPeer{
		String msg  = null;
		Request req = new Request();
		req.setData(query);
		req.setMethod(Method.query);
		try{
			msg = objMapper.writeValueAsString(req);			
		}catch(Exception e){
			logger.error("failed to convert query to String !",e);
		}
		return this.put(msg);
	}
	
	/**
	 * 发送请求并等待返回
	 * @param requestStr
	 * @return
	 * @throws TimeoutException
	 * @throws ConnectClosedByPeer
	 */
	private String put(String requestStr)throws TimeoutException,ConnectClosedByPeer{
		if(requestStr==null)
			return null;
		
		synchronized(this){
			if(channel==null )
				throw new ConnectClosedByPeer("connection may be closed by peer!");
			this.state = State.send;
			byte[] bytes = requestStr.getBytes();
			ByteBuf encoded = channel.alloc().buffer(bytes.length);    
	        encoded.writeBytes(bytes);    
	        encoded.writeByte('\r');
	        encoded.writeByte('\n');
	        channel.write(encoded);    
	        channel.flush();
	        logger.debug("数据已提交");
	        this.state = State.wait;
		
			while(state.equals(State.idle)==false){
				try {
					this.wait(this.timeout4Response);
					break;
				} catch (InterruptedException e) {
					logger.error("wait is interrupted!",e);
				}				
			}
			if(state.equals(State.idle)==false) {
				try {
					channel.close();
				}catch(Exception e) {
					logger.error("failed to close ctx!",e);
				}
		        channel = null;
				throw new TimeoutException("time out for Response");
			}else {
				String res = this.response;
				return res;
			}
		}
	}
	
	
	public void channelRead(ChannelHandlerContext ctx, Object msg){	 	
	 	synchronized(this){
	 		this.state = State.receive;
	 		this.response = (String)msg;
	 		this.state = State.idle;
	 		this.notify();
	 	}
	}
	
	@Override  
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		synchronized(this){
			logger.error("caught exception of channel! ",cause);
			this.notify();
			try {
				channel.close();
			}catch(Exception e) {
				logger.error("failed to close channel!",e);
			}
	        channel = null;
		}
    }
}
