# Binance USDⓈ-M iceberg/replenishment detector

FlowSurface can mark **possible** passive iceberg activity on Binance USDⓈ-M linear perpetuals. The detector is disabled by default and is not supported for Binance Spot, COIN-M, or other venues yet.

## What it detects

The engine looks for repeated aggressive executions at a visible bid or ask followed by restoration of displayed quantity while price fails to advance through that level. A sell aggressor absorbed at a bid is reported as a `Possible buy iceberg`; a buy aggressor absorbed at an ask is a `Possible sell iceberg`. It can also be described as bid/ask replenishment or passive buy/sell absorption.

This is probabilistic market-structure evidence, not proof of an exchange-native iceberg order. Binance public L2 data does not expose order identities, queue position, hidden quantities, RPI/private liquidity, or the intent of a participant. Multiple ordinary limit orders can create the same pattern.

## Inputs and local book

Live detection uses `<symbol>@trade` and `<symbol>@depth@100ms`. Raw `trade` preserves trade ID, event time, trade time and the buyer-maker flag. With Binance `m=true`, the buyer was maker and the aggressor was a seller; `m=false` means a buyer aggressor. The raw live stream also feeds bubbles, footprint and CVD. Historical download remains based on `aggTrades`; because raw and aggregate IDs are different namespaces, historical records are not mixed into time buckets already populated by the live raw feed. Unlike raw trades, an aggregate trade may combine executions belonging to one taker order.

The depth adapter opens and buffers the diff stream, fetches a REST snapshot, discards updates older than the snapshot boundary, finds the first covering update, and then checks `pu` against the preceding `u`. Every normalized level delta contains both the previous local quantity and the new absolute quantity. A sequence gap, reconnect, bootstrap, or resync invalidates open episodes. Detection resumes only after a valid snapshot boundary and continuous sequence.

Exchange event/transaction time is the primary correlation clock. Receive time, captured as the WebSocket message enters the adapter, is the tie breaker. Only the detector uses a bounded 150 ms reorder buffer; legacy depth rendering and trade batching remain direct.

## Refill accounting

For each passive side and price, the engine retains visible quantity, pending aggressive execution, expected quantity after those executions, and last hit time. Multiple trades before one depth update are accumulated. On the next coherent delta:

```text
expected_after = max(previous_visible - pending_executed, 0)
replenished    = max(current_visible - expected_after, 0)
cancelled      = max(previous_visible
                     - min(previous_visible, pending_executed)
                     - current_visible, 0)
```

This prevents already-visible liquidity from being counted as a refill. Insertions without a preceding hit do not form a refill cycle. Unexplained decreases are cancellation evidence and reduce the score.

## Adaptive baseline and score

The rolling 60-second baseline includes trade-size median/p75 and median touch depth. No signal is emitted before the minimum warm-up observations exist. The opening threshold is the maximum of five minimum-quantity lots, trade-size p75, and 25% of median touch depth. No BTC-specific quantity is hardcoded.

The deterministic score is clamped to 0–100 and combines executed/displayed ratio (22), refill ratio (18), refill count (18), latency (12), displayed-clip consistency (10), resistance to adverse movement (15), and persistence (5). Cancellation can subtract 20 and poor data quality up to 30. This score is an explainable heuristic, not a calibrated probability.

Defaults require three refill cycles, executed/displayed >= 2.5, refill ratio >= 0.50, at most one adverse tick, and score >= 70. A stable event ID is updated rather than creating a marker for each refill. `hidden_lower_bound_qty` means only “minimum absorbed beyond peak displayed” (`max(executed - peak displayed, 0)`); it is not an estimate of total hidden size.

## UI and configuration

Heatmap settings contain **Possible iceberg markers (Binance Linear)**. Defaults are disabled, two ticks from touch, 150 ms reorder, 5 s idle timeout, 30 s maximum episode, score 70, and five-minute marker retention. Legacy serialized layouts omit the nested configuration and therefore load it disabled.

Visible classic-heatmap events use a small upward triangle for possible buy icebergs and downward triangle for possible sell icebergs. Size is capped and based on score; opacity reflects feed quality. Weak candidates are hidden unless explicitly enabled in configuration.

The runtime registry is keyed by exchange, ticker, market kind (through `TickerInfo`) and tick size. It creates one detector for all matching panes, reference-counts consumers, and releases it after a short grace period when the last pane disables or leaves it.

## Recording and replay

`OrderFlowRecorder` writes optional JSONL under a caller-selected `logs/orderflow` directory. Records include session metadata, snapshot boundaries, depth deltas, trades, gaps, reconnects and detector output. It is never enabled by default. `replay_jsonl` feeds stored timestamps and receive-time offsets through the same reorder and detector logic, without wall-clock calls, so identical input and configuration produce identical output.

Raw records can be large and may contain market activity; configure retention outside the detector. Normal application logs record lifecycle, warm-up, gaps and confirmations, not every trade or delta.

## Limits and false positives

Likely false positives include several participants independently replacing liquidity, market-maker quoting rules, rapid cancel/replace, feed delay, price aggregation, and an incomplete public depth view. Sweeps, distant levels, large trades without refill, refill without a prior execution, high cancellation ratios, duplicate IDs, gaps, reconnects, snapshot changes, microscopic sizes and levels broken beyond the adverse-tick limit are explicitly rejected or penalized.

Supporting another exchange requires an adapter that provides raw trade identity/aggressor semantics, absolute L2 deltas with sequence continuity, snapshot boundaries, transaction/event/receive timestamps, and explicit quality transitions. The pure detector and shared registry do not depend on `iced`.
