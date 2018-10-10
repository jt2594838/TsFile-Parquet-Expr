package expr;

public class MonitorThread extends Thread {

    private long interval = 10; // ms
    private long maxMemUsage;

    @Override
    public void run() {
        Runtime runtime = Runtime.getRuntime();
        while (true) {
            if (this.isInterrupted()) {
                break;
            }
            long memUsage = runtime.totalMemory() - runtime.freeMemory();
            maxMemUsage = memUsage > maxMemUsage ? memUsage : maxMemUsage;
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public long getMaxMemUsage() {
        return maxMemUsage;
    }
}
