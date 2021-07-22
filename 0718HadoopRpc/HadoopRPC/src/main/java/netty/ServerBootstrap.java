package netty;

public class ServerBootstrap {
    public static void main(String[] args) {
        UserServiceImpl.startServer("127.0.0.1", 9999);
    }
}
