"use client";

import { useMemo, useState } from "react";

import { EvidenceItem } from "../lib/mas-types";
import { EmptyState } from "./empty-state";
import { JsonViewer } from "./json-viewer";

export function EvidencePanel({ evidence }: { evidence: EvidenceItem[] }) {
  const [keyword, setKeyword] = useState("");

  const filtered = useMemo(() => {
    if (!keyword.trim()) return evidence;
    const lower = keyword.toLowerCase();
    return evidence.filter((item) => {
      const source = (item.source || "").toLowerCase();
      const title = (item.title || "").toLowerCase();
      const snippet = (item.snippet || "").toLowerCase();
      return source.includes(lower) || title.includes(lower) || snippet.includes(lower);
    });
  }, [evidence, keyword]);

  if (evidence.length === 0) {
    return <EmptyState title="暂无证据包" description="当前运行没有暴露 evidence 列表。" />;
  }

  return (
    <div className="stack-gap-sm">
      <input value={keyword} onChange={(event) => setKeyword(event.target.value)} placeholder="搜索证据来源/内容" />
      <div className="stack-gap-sm">
        {filtered.map((item, index) => (
          <article key={`${item.id || item.source || "evidence"}-${index}`} className="sub-panel stack-gap-xs">
            <div className="sub-panel-head">
              <strong>{item.title || item.source || `证据 ${index + 1}`}</strong>
              {item.conflict ? <span className="tag tag-danger">冲突</span> : null}
            </div>
            {item.snippet ? <p className="muted-text">{item.snippet}</p> : null}
            <JsonViewer value={item.raw || item} title="原始内容" />
          </article>
        ))}
      </div>
    </div>
  );
}
