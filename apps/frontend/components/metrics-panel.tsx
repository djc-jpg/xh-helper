"use client";

import { Metrics } from "../lib/mas-types";
import { formatCurrency, formatNumber } from "../lib/format";
import { JsonViewer } from "./json-viewer";

export function MetricsPanel({ metrics }: { metrics: Metrics }) {
  return (
    <div className="stack-gap-md">
      <div className="grid cols-2">
        <div className="sub-panel">
          <p className="panel-subtitle">消息总量</p>
          <p className="metric-value">{formatNumber(metrics.messageTotal, 0)}</p>
        </div>
        <div className="sub-panel">
          <p className="panel-subtitle">耗时（ms）</p>
          <p className="metric-value">{formatNumber(metrics.elapsedMs, 0)}</p>
        </div>
        <div className="sub-panel">
          <p className="panel-subtitle">token_in</p>
          <p className="metric-value">{formatNumber(metrics.tokenIn, 0)}</p>
        </div>
        <div className="sub-panel">
          <p className="panel-subtitle">token_out</p>
          <p className="metric-value">{formatNumber(metrics.tokenOut, 0)}</p>
        </div>
        <div className="sub-panel">
          <p className="panel-subtitle">总成本</p>
          <p className="metric-value">{formatCurrency(metrics.totalCost)}</p>
        </div>
      </div>
      <JsonViewer value={metrics.raw} title="metrics 原始 JSON" />
    </div>
  );
}
