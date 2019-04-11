package org.hmf.tsdb.server;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.hmf.tsdb.DataPoint;
import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.Response;
import org.hmf.tsdb.server.service.DataPointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * 连接服务器端处理器
 */
public class ServerHandler extends ChannelInboundHandlerAdapter{
	private Logger logger = LoggerFactory.getLogger(ServerHandler.class);
	ObjectMapper objMapper = new ObjectMapper();
	private DataPointService service ;
	
	public ServerHandler() {
		service = (DataPointService) TsdbApplication.getBean(DataPointService.class);
	}
	
	private Response<Boolean> doPut(String group,JsonNode root,ChannelHandlerContext ctx)throws Exception{		
		Response<Boolean> response = null;
		try {
			List<DataPoint> dps = new ArrayList<DataPoint>();
			long t0 = System.currentTimeMillis();
			Iterator<JsonNode> iter = root.iterator();
			while(iter.hasNext()){
				JsonNode node = iter.next();
				DataPoint dp = null;	
				dp = objMapper.readValue(new TreeTraversingParser(node), DataPoint.class);	
				dps.add(dp);
			}
			long t1 = System.currentTimeMillis();
			logger.debug("反序列化，得到{}个DataPoint,shard={},time={}",dps.size(),group,(t1-t0));
			service.acceptDataPointList(dps);
			response = Response.success(true);
		}catch(Exception e) {
			logger.error("保存失败！"+e.getMessage());
		}
		
		return response;
	}
	
	
	private void doResponse(Response<?> response,ChannelHandlerContext ctx){
		String rStr = null;
		try {
			rStr = objMapper.writeValueAsString(response);
		} catch (JsonProcessingException e) {
			logger.warn("failed to convert Result to Json String!",e);
		}
		
		byte[] bytes = rStr.getBytes();
		ByteBuf encoded = ctx.alloc().buffer(bytes.length);    
        encoded.writeBytes(bytes);    
        encoded.writeByte('\r');
        encoded.writeByte('\n');
        ctx.write(encoded);    
        ctx.flush();
	}
	
	private Response<List<DataPointSerial>> doQuery(JsonNode root,ChannelHandlerContext ctx)throws Exception{
		Response<List<DataPointSerial>> response = null;	
		String exp = root.asText();
		try {
			List<DataPointSerial> list = service.queryByExp(exp);
			response = Response.success(list);
		}catch(Exception e) {
			logger.error("查询失败，查询语句={}",exp,e);
			response = Response.error("查询失败："+e.getMessage(), null);
		}
		return response;
	}
	
	long lastReadAt = 0;
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
		Response<?> response = null;
		try{
			logger.debug("recieved message from {}:{}",this.getRemoteIp(ctx),message );	
			JsonNode root =objMapper.readTree((String)message);
			String method = root.get("method").asText();
			if(method==null || method.isEmpty()){
				throw new Exception("method is null or empty!");
			}
			if(method.equals("put")){
				String group = root.get("group")==null?"default":root.get("group").asText();
				response = doPut(group,root.get("data"),ctx);
			}else if(method.equals("query")){
				response = doQuery(root.get("data"),ctx);
			}else {
				throw new Exception("unknow method");
			}
		}catch(Exception e){
			logger.error("faile to deal request:",e);
			response = Response.error(e.getMessage(),null);
		}
		this.doResponse(response, ctx);
	}
	

	private String getRemoteIp (ChannelHandlerContext ctx){
		InetSocketAddress insocket = (InetSocketAddress) ctx.channel().remoteAddress();
		String clientIp = insocket.getAddress().getHostAddress();
		return clientIp;
	}
	

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {		
        logger.info("connected from "+this.getRemoteIp(ctx));
	}



	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {		
		try {
	        logger.info("disconnected from "+this.getRemoteIp(ctx));
	        ctx.close();
		}catch(Exception e) {
			logger.error("",e);
		}
	}



	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		try {
			logger.error(evt.toString());
			ctx.close();
		}catch(Exception e) {
			logger.error("",e);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		try {
			logger.error("error happened on "+ this.getRemoteIp(ctx) + " :{}",cause.getMessage());
			ctx.close();
		}catch(Exception e) {
			logger.error("",e);
		}
	}


	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {	
		try {
	        ctx.close();
		}catch(Exception e) {
			logger.error("",e);
		}
	}
}
