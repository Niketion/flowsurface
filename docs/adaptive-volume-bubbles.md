# Adaptive Volume Bubbles

Volume Bubbles visualize bursts of aggressive executions rather than simple price bins. A trade is
a single execution; a price bin sums every execution at the same price; a smart cluster instead
combines only trades from the same candle that are close in time and price. Each cluster preserves
its VWAP, volume-weighted time, buy and sell volume, delta, trade count, and largest trade.

The pure `cluster_volume_bubble_trades` pipeline is shared by live data, historical data, and replay.
When complete raw trades are available, they take precedence; otherwise, only the pre-clustered v2
summary is used. Raw data and summaries are never added together. V1 summaries, which are based on
candle and price, live in a separate table and are not interpreted as v2 clusters.

## Thresholds and stability

- **Fixed** uses `min_qty`.
- **AdaptivePercentile** uses the rolling cluster percentile over the configured window.
- **Hybrid** uses the greater of the percentile and `min_qty`.

During warm-up, the absolute floor prevents NaN values and unstable thresholds. The live threshold
is updated at most once per second and only when it changes by at least 10%. A cluster ID is derived
from the burst's stable anchors, so it does not change while the current cluster grows. A per-side
baseline can be used when enough samples are available; otherwise, the combined distribution is
used.

## Visual hierarchy

Volume and percentile dominate the importance score; dominance and trade count add only small,
explainable bonuses. After thresholding, the renderer applies the per-candle budget, global viewport
budget, deterministic horizontal collision handling, and label budget. `ExtremeOnly` labels only
events above the label percentile; the remaining bubbles are rendered without text.

The radius uses logarithmic compression relative to the threshold and interpolation over the
circle's area. It remains within the configured limits and is resistant to outliers. A transparent
fill, more legible outline, and theme-derived colors preserve candles, wicks, and overlays. Clusters
with weak dominance are neutral. Age fading decreases gradually to about 58% without hiding
historical bubbles.

## Price response

The optional analysis classifies the response after a fully elapsed horizon as `FollowThrough`,
`Stalled`, `Reversed`, `Pending`, or `Neutral`. It does not use future data in live mode and remains
secondary to volume. `Stalled` can be consistent with passive absorption, but it does not prove it
and does not automatically identify an iceberg.

## Interpretation limits

A bubble represents aggregated aggressive activity. It does not automatically identify whether a
position was opened or closed. Results depend on the quality, ordering, and completeness of the
trades supplied by the exchange; incomplete intrabar data reduces the temporal precision of
clustering.
