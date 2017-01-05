package org.rocksdb;

/**
 * @author jie.huang
 *         Date: 16/11/15
 *         Time: 下午7:10
 */
public class MetricsScanner extends RocksObject {
    private RocksDB rocksDB;

    private int metric = -1;
    private int start = -1;
    private int end = -1;
    private int maxPointCount;
    private byte[] tagFilters;
    private byte[] groupBy;
    private boolean enableLog;
    private boolean enableProfiler;
    private boolean init = true;

    public MetricsScanner(RocksDB rocksDB, int maxPointCount, long nativeHandle) {
        super(nativeHandle);
        this.rocksDB = rocksDB;
        this.maxPointCount = maxPointCount;
    }

    public void setMetric(int metric) {
        this.metric = metric;
    }

    public void setEnableLog(boolean enableLog){
        this.enableLog = enableLog;
    }

     public void setEnableProfiler(boolean enableProfiler){
            this.enableProfiler = enableProfiler;
        }

    public void setRange(int start,int end) {
        this.start = start;
        this.end = end;
    }

    public void setTagFilters(byte[] tagFilters){
        this.tagFilters = tagFilters;
    }

    public void setGroupBy(byte[] groupBy){
        this.groupBy = groupBy;
    }

    public void next() throws RocksDBException {
        if(init){
            if (metric == -1) {
                throw new RocksDBException("Metric cannot be empty.");
            }
            if (start == -1) {
                throw new RocksDBException("Start slot cannot be empty.");
            }
            if (end == -1) {
                throw new RocksDBException("End slot cannot be empty.");
            }

            if (maxPointCount <= 0) {
                throw new RocksDBException("Max point count must be > 0.");
            }
            maxPointCount(nativeHandle_, maxPointCount);
            metric(nativeHandle_, metric);
            start(nativeHandle_, start);
            end(nativeHandle_, end);
            if(tagFilters!=null && tagFilters.length>0){
                setTagFilters(nativeHandle_, tagFilters.length, tagFilters);
            }
            if(groupBy!=null && groupBy.length>0){
                setGroupBy(nativeHandle_, groupBy.length, groupBy);
            }
            enableLog(nativeHandle_,enableLog);
            enableProfiler(nativeHandle_,enableProfiler);
            init = false;
        }

        next(nativeHandle_);
    }

    public boolean hasNext() {
        return hasNext(nativeHandle_);
    }

    public int getCurrentBaseTime(){
        return getCurrentBaseTime(nativeHandle_);
    }

    public byte[] getResultSet() {
        return getResultSet(nativeHandle_);
    }

    public byte[] getGroupBy(){
        return getGroupBy(nativeHandle_);
    }

    public byte[] getStat(){
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

    private native void start(long handle, int start);

    private native void end(long handle, int end);

    private native void next(long handle);

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
