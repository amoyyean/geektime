package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class RPCServerDriver {
    public static void main(String[] args) {
        // 创建RPC的配置
        Configuration configuration = new Configuration();
        // 构建RPC的builder对象
        RPC.Builder builder = new RPC.Builder(configuration);
        // 设置RPC Server的信息，返回一个server对象

        builder.setBindAddress("127.0.0.1") // 服务器IP地址
                .setPort(9999) // 服务器端口号
                .setProtocol(MyInterface.class)
                .setInstance(new MyInterfaceImpl());
        try {
            RPC.Server server = builder.build() ;
            // 启动RPC Server
            // 这是一个守护进程，所以main函数不会退出
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("--- Hadoop RPC 服务已经启动 ---");

    }
}
