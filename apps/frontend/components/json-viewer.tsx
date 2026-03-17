"use client";

import Prism from "prismjs";
import "prismjs/components/prism-json";
import { useMemo, useState } from "react";

import { safeStringify } from "../lib/mas-utils";

function filterJsonByKeyword(json: string, keyword: string): string {
  if (!keyword.trim()) return json;
  const lines = json.split("\n");
  const lower = keyword.toLowerCase();
  return lines.filter((line) => line.toLowerCase().includes(lower)).join("\n") || "{}";
}

export function JsonViewer({
  value,
  title,
  defaultExpanded = false,
  className = ""
}: {
  value: unknown;
  title?: string;
  defaultExpanded?: boolean;
  className?: string;
}) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const [keyword, setKeyword] = useState("");

  const raw = useMemo(() => safeStringify(value), [value]);
  const filtered = useMemo(() => filterJsonByKeyword(raw, keyword), [raw, keyword]);
  const highlighted = useMemo(
    () => Prism.highlight(filtered, Prism.languages.json, "json"),
    [filtered]
  );

  return (
    <section className={`json-viewer ${className}`.trim()}>
      <header className="json-toolbar">
        <div className="json-title-wrap">
          {title ? <h4 className="panel-title">{title}</h4> : null}
          <button type="button" className="btn btn-ghost" onClick={() => setExpanded((prev) => !prev)}>
            {expanded ? "折叠" : "展开"}
          </button>
        </div>
        <div className="json-actions">
          <input
            value={keyword}
            onChange={(event) => setKeyword(event.target.value)}
            placeholder="搜索 key/value"
            aria-label="search json"
          />
          <button
            type="button"
            className="btn btn-ghost"
            onClick={() => navigator.clipboard.writeText(filtered).catch(() => undefined)}
          >
            复制
          </button>
        </div>
      </header>
      {expanded ? (
        <pre className="code-block" aria-label="json content">
          <code dangerouslySetInnerHTML={{ __html: highlighted }} />
        </pre>
      ) : null}
    </section>
  );
}
