package netty;

public class ClientBootstrap {

    public static void main(String[] args) throws InterruptedException {
        RpcConsumer consumer = new RpcConsumer();
        // 创建一个代理对象
        UserService service = (UserService) consumer.createProxy(UserService.class);
        while(true) {
            Thread.sleep(1000);
            System.out.println(service.findName(20210123456789L));
        }
    }
}
