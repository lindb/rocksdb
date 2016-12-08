package org.rocksdb;

/**
 * @author jie.huang
 *         Date: 16/11/15
 *         Time: 下午7:10
 */
public class MetricsScanner extends RocksObject {
    private RocksDB rocksDB;

    private int metric = -1;
    private int startHour = -1;
    private int endHour = -1;
    private int maxPointCount;

    public MetricsScanner(RocksDB rocksDB, int maxPointCount, long nativeHandle) {
        super(nativeHandle);
        this.rocksDB = rocksDB;
        this.maxPointCount = maxPointCount;
    }

    public void setMetric(int metric) {
        this.metric = metric;
    }

    public void setHourRange(int startHour,int endHour) {
        this.startHour = startHour;
        this.endHour = endHour;
    }

    public void next() throws RocksDBException {
        if (metric == -1) {
            throw new RocksDBException("Metric cannot be empty.");
        }
        if (startHour == -1) {
            throw new RocksDBException("Start hour cannot be empty.");
        }
        if (endHour == -1) {
            throw new RocksDBException("End hour cannot be empty.");
        }

        if (maxPointCount <= 0) {
            throw new RocksDBException("Max point count must be > 0.");
        }
        maxPointCount(nativeHandle_, maxPointCount);
        metric(nativeHandle_, metric);
        startHour(nativeHandle_, startHour);
        endHour(nativeHandle_, endHour);

        next(nativeHandle_);
    }

    public boolean hasNext() {
        return hasNext(nativeHandle_);
    }

    public int getCurrentHour(){
        return getCurrentHour(nativeHandle_);
    }

    public byte[] getResultSet() {
        return getResultSet(nativeHandle_);
    }

    public void close() {
        super.close();
    }

    @Override
    protected void disposeInternal() {
        if (rocksDB.isOwningHandle()) {
            disposeInternal(nativeHandle_);
        }
    }

    private native void maxPointCount(long handle, int maxPointCount);

    private native void metric(long handle, int metric);

    private native void startHour(long handle, int startHour);

    private native void endHour(long handle, int endHour);

    private native void next(long handle);

    private native boolean hasNext(long handle);

    private native int getCurrentHour(long handle);

    private native byte[] getResultSet(long handle);

    @Override
    protected final native void disposeInternal(final long handle);
}
