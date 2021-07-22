
public class ThreadTask implements Runnable {
    private  int i ;
    public ThreadTask(int i ){
        this.i = i ;
    }

    @Override
    public void run() {
        sayHello(this.i);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sayHello(int i) {
        System.out.println(Thread.currentThread() + "  , i = " + i);
    }
}
