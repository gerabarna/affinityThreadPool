package hu.gerab.concurrent.taskAffinity;

public class TestData {

    private final String threadName;
    private final String affinityGroup;
    private final int serial;

    public TestData(String affinityGroup, int serial) {
        this.threadName = Thread.currentThread().getName();
        this.affinityGroup = affinityGroup;
        this.serial = serial;
    }

    public String getThreadName() {
        return threadName;
    }

    public String getAffinityGroup() {
        return affinityGroup;
    }

    public int getSerial() {
        return serial;
    }
}
