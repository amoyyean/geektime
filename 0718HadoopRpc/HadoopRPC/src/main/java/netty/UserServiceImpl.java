package netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

public class UserServiceImpl implements UserService {
    @Override
    public  String findName(long studentId){
        if (studentId == 20210123456789L) {
            System.out.println("studentId = " + studentId);
            return "心心" ;
        }else
            System.out.println("Error Student ID = " + studentId);
        return "null" ;
    }

    public static void startServer(String hostName, int port) {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
            bootstrap.group(eventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new StringDecoder());
                            p.addLast(new StringEncoder());
                            p.addLast(new UserServerHandler());
                        }
                    });
            bootstrap.bind(hostName, port).sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
