package com.imooc.transfer;

import com.youfan.entity.KafkaMessage;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class KafkaMessageWatermarks implements AssignerWithPeriodicWatermarks<KafkaMessage> {

    private  long currentTime = Long.MIN_VALUE;
    /**
     * Returns the current watermark. This method is periodically called by the
     * system to retrieve the current watermark. The method may return {@code null} to
     * indicate that no new Watermark is available.
     *
     * <p>The returned watermark will be emitted only if it is non-null and its timestamp
     * is larger than that of the previously emitted watermark (to preserve the contract of
     * ascending watermarks). If the current watermark is still
     * identical to the previous one, no progress in event time has happened since
     * the previous call to this method. If a null value is returned, or the timestamp
     * of the returned watermark is smaller than that of the last emitted one, then no
     * new watermark will be generated.
     *
     * <p>The interval in which this method is called and Watermarks are generated
     * depends on {@link //ExecutionConfig#getAutoWatermarkInterval()}.
     *
     * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
     * @see Watermark
     * @see //ExecutionConfig#getAutoWatermarkInterval()
     */
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTime  == Long.MIN_VALUE?Long.MIN_VALUE:currentTime - 1);
    }

    /**
     * Assigns a timestamp to an element, in milliseconds since the Epoch.
     *
     * <p>The method is passed the previously assigned timestamp of the element.
     * That previous timestamp may have been assigned from a previous assigner,
     * by ingestion time. If the element did not carry a timestamp before, this value is
     * {@code Long.MIN_VALUE}.
     *
     * @param element                  The element that the timestamp will be assigned to.
     * @param previousElementTimestamp The previous internal timestamp of the element,
     *                                 or a negative value, if no timestamp has been assigned yet.
     * @return The new timestamp.
     */
    @Override
    public long extractTimestamp(KafkaMessage element, long previousElementTimestamp) {
        this.currentTime = element.getTimestamp();
        return element.getTimestamp();
    }
}
