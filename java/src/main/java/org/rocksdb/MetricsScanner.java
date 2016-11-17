package org.rocksdb;

/**
 * @author jie.huang
 *         Date: 16/11/15
 *         Time: 下午7:10
 */
public class MetricsScanner extends RocksObject {
    private RocksDB rocksDB;
    private int metricsId = -1;
    private int time = -1;
    private int startSlot = -1;
    private int endSlot = -1;
    private boolean count = false;
    private boolean sum = false;
    private boolean min = false;
    private boolean max = false;

    public MetricsScanner(RocksDB rocksDB, long nativeHandle) {
        super(nativeHandle);
        this.rocksDB = rocksDB;
    }

    public void setMetricsId(int metricsId) {
        this.metricsId = metricsId;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public void enableCount(boolean enable) {
        this.count = enable;
    }

    public void enableSum(boolean enable) {
        this.sum = enable;
    }

    public void enableMin(boolean enable) {
        this.min = enable;
    }

    public void enableMax(boolean enable) {
        this.max = enable;
    }

    public void setSlotRange(int startSlot, int endSlot) {
        this.startSlot = startSlot;
        this.endSlot = endSlot;
    }

    public void doScan() throws RocksDBException {
        if (metricsId == -1) {
            throw new RocksDBException("Metrics id cannot be null.");
        }
        if (time == -1) {
            throw new RocksDBException("Time cannot be null.");
        }

        if (!count && !sum && !min && !max) {
            throw new RocksDBException("Aggregator function cannot be null.");
        }
        if (startSlot == -1 || endSlot == -1) {
            throw new RocksDBException("Slot range invalid.");
        }

        setMetrics(nativeHandle_, metricsId);

        setTime(nativeHandle_, time);

        setSlotRange(nativeHandle_, startSlot,endSlot);

        enableCount(nativeHandle_, count);
        enableSum(nativeHandle_, sum);
        enableMin(nativeHandle_, min);
        enableMax(nativeHandle_, max);

        doScan(nativeHandle_);
    }

    public byte[] getCountResult() {
        return getCountResult(nativeHandle_);
    }

    public byte[] getSumResult() {
        return getSumResult(nativeHandle_);
    }

    public byte[] getMinResult() {
        return getMinResult(nativeHandle_);
    }

    public byte[] getMaxResult() {
        return getMaxResult(nativeHandle_);
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

    private native void setMetrics(long handle, int metrics);

    private native void setTime(long handle, int metrics);

    private native void enableCount(long handle, boolean enable);

    private native void enableSum(long handle, boolean enable);

    private native void enableMin(long handle, boolean enable);

    private native void enableMax(long handle, boolean enable);

    private native void setSlotRange(long handle, int startSlot,int endSlot);

    private native void doScan(long handle);

    private native byte[] getCountResult(long handle);

    private native byte[] getSumResult(long handle);

    private native byte[] getMinResult(long handle);

    private native byte[] getMaxResult(long handle);

    @Override
    protected final native void disposeInternal(final long handle);
}
