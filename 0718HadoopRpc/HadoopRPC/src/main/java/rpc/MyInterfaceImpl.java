package rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class MyInterfaceImpl implements MyInterface {
    @Override
    public int add(int num1, int num2) {
        System.out.println("num1 = " + num1 + ", num2 = " + num2);
        return num1 + num2 ;
    }

    public  String findName(long studentId){
        if (studentId == 20210123456789L) {
            System.out.println("studentId = " + studentId);
            return "心心" ;
        }else
            System.out.println("Error Student ID = " + studentId);
            return null ;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return MyInterface.versionID ;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}
