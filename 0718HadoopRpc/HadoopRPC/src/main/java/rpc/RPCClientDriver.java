package rpc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * RPC客户端程序
 */
public class RPCClientDriver {
    public static void main(String[] args)  {
        // 构建InetSocketAddress对象
        InetSocketAddress address = null;
        try {
            address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9999);
            // 通过RPC.getProxy方法获得代理对象
            /**
             * @param protocol      接口的类型对象
             * @param clientVersion 版本号
             * @param addr          服务端地址
             * @param conf          配置信息
             */
            MyInterface myServiceProxy = RPC.getProxy(MyInterface.class, MyInterface.versionID, address, new Configuration());
            String result = myServiceProxy.findName(20210123456789L);
            System.out.println(result);
            RPC.stopProxy(myServiceProxy);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
