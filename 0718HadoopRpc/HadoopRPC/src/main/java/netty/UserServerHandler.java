package netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class UserServerHandler  extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        String result = new UserServiceImpl()
                    .findName(Long.parseLong(msg.toString()) );
        ctx.writeAndFlush(result);

    }
}
