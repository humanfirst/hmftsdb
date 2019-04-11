package org.hmf.tsdb.server;
import java.nio.charset.Charset;

import javax.annotation.PreDestroy;

import org.hmf.tsdb.server.service.DataPointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.timeout.IdleStateHandler;

@Component
public class Server implements ApplicationListener<ContextRefreshedEvent>{
	private static Logger logger = LoggerFactory.getLogger(Server.class);
	
	private boolean closed = false;
	
	private EventLoopGroup bossGroup = null;
	
	private EventLoopGroup workerGroup = null;
	
	@Autowired
	private DataPointService service;

	@Autowired
	private ServerNode node = null;
	
	public Server(){
	}

	public boolean isClosed() {
		return closed;
	}

	@PreDestroy
	public void destory(){
		this.shutDown();
	}
	
	public void shutDown(){
		try {
			logger.info("正在关闭网络服务...");
			if(workerGroup!=null)
				workerGroup.shutdownGracefully();
			if(bossGroup!=null)
				bossGroup.shutdownGracefully();
	        this.closed = true;
	        logger.info("网络服务已关闭");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void startServer(){		
		bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        try {
        	logger.info("to init server...");
            ServerBootstrap b = new ServerBootstrap(); 
            b.group(bossGroup, workerGroup)
             	.channel(NioServerSocketChannel.class) 
             	.childHandler(new ChannelInitializer<SocketChannel>() { 
	                 @Override
	                 public void initChannel(SocketChannel ch) throws Exception {
	                	 logger.info("init server channel!");
	                	 ch.pipeline().addLast(new IdleStateHandler(0,0,60));
	                	 ch.pipeline().addLast(new LineBasedFrameDecoder(100*1024*1024));
	                	 ch.pipeline().addLast(new StringDecoder(Charset.forName("UTF-8")));
	                     ch.pipeline().addLast(new ServerHandler(service));
	                 }
	             })
             	.option(ChannelOption.SO_BACKLOG, 512)          
             	.childOption(ChannelOption.SO_KEEPALIVE, true); 
            
            ChannelFuture f = b.bind(node.getServiceHost(),node.getServicePort()).sync();
            logger.info("start server finished! linstener on host:{} ,port {}",
            		node.getServiceHost(), node.getServicePort());
            f.channel().closeFuture().sync();
            logger.info("server is closed");
        }catch(Exception e){
        	e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
	}

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		Thread t = new Thread(()->{
			startServer();
		}) ;
		t.start();		
	}
}
