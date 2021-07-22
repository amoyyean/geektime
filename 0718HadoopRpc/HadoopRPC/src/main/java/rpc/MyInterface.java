package rpc;

import org.apache.hadoop.ipc.VersionedProtocol;
/**
 * 接口
 * 使用hadoop提供的RPC服务，要继承VersionedProtocol
 */
public interface MyInterface extends VersionedProtocol {
    // 想要被序列化必须声明 versionID
    long versionID = 1L ;
    int add(int num1 , int num2) ;
    String findName(long studentId) ;
}
