package org.rocksdb;

/**
 * @author jie.huang
 *         Date: 16/11/15
 *         Time: 下午7:10
 */
public class MetricsScanner extends RocksObject {
    private RocksDB rocksDB;

    public MetricsScanner(RocksDB rocksDB, long nativeHandle) {
        super(nativeHandle);
        this.rocksDB = rocksDB;
    }

    public void setMaxPointCount(int maxPointCount) throws RocksDBException {
        if (maxPointCount <= 0) {
            throw new RocksDBException("Max point count must be > 0.");
        }
        maxPointCount(nativeHandle_, maxPointCount);
    }

    public void setMetricType(byte metricType) throws RocksDBException {
        if (metricType <= 0) {
            throw new RocksDBException("Metric type cannot be empty.");
        }
        metricType(nativeHandle_, metricType);
    }

    public void setMetric(int metric) throws RocksDBException {
        if (metric == -1) {
            throw new RocksDBException("Metric cannot be empty.");
        }
        metric(nativeHandle_, metric);
    }

    public void setEnableLog(boolean enableLog) {
        enableLog(nativeHandle_, enableLog);
    }

    public void setEnableProfiler(boolean enableProfiler) {
        enableProfiler(nativeHandle_, enableProfiler);
    }

    public void setRange(int start, int end) throws RocksDBException {
        if (start == -1) {
            throw new RocksDBException("Start slot cannot be empty.");
        }
        if (end == -1) {
            throw new RocksDBException("End slot cannot be empty.");
        }
        start(nativeHandle_, start);
        end(nativeHandle_, end);
    }

    public void setMinTagValueLen(int minTagValueLen) {
        minTagValueLen(nativeHandle_, (byte) minTagValueLen);
    }

    public void setTagFilters(byte[] tagFilters) {
        if (tagFilters != null && tagFilters.length > 0) {
            setTagFilters(nativeHandle_, tagFilters.length, tagFilters);
        }
    }

    public void setGroupBy(byte[] groupBy) {
        if (groupBy != null && groupBy.length > 0) {
            setGroupBy(nativeHandle_, groupBy.length, groupBy);
        }
    }

    public void next() {
        next(nativeHandle_);
    }

    public boolean hasNextBaseTime(byte baseTime, ColumnFamilyHandle familyHandle) {
        return hasNextBaseTime(nativeHandle_, baseTime, familyHandle.nativeHandle_);
    }

    public boolean hasNext() {
        return hasNext(nativeHandle_);
    }

    public int getCurrentBaseTime() {
        return getCurrentBaseTime(nativeHandle_);
    }

    public byte[] getResultSet() {
        return getResultSet(nativeHandle_);
    }

    public byte[] getGroupBy() {
        return getGroupBy(nativeHandle_);
    }

    public byte[] getStat() {
        return getStat(nativeHandle_);
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

    private native void enableLog(long handler, boolean enable);

    private native void enableProfiler(long handler, boolean enable);

    private native void maxPointCount(long handle, int maxPointCount);

    private native void metric(long handle, int metric);

    private native void metricType(long handle, byte metricType);

    private native void minTagValueLen(long handle, byte minTagValueLen);

    private native void start(long handle, int start);

    private native void end(long handle, int end);

    private native void next(long handle);

    private native boolean hasNextBaseTime(long handle, byte baseTime, long columnFamilyHandle);

    private native boolean hasNext(long handle);

    private native int getCurrentBaseTime(long handle);

    private native byte[] getResultSet(long handle);

    private native byte[] getGroupBy(long handle);

    private native byte[] getStat(long handle);

    private native void setTagFilters(long handle, int len, byte[] tagFilters);

    private native void setGroupBy(long handle, int len, byte[] groupBy);

    @Override
    protected final native void disposeInternal(final long handle);
}
