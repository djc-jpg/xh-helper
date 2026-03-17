export function formatDateTime(value?: string | null, locale = "zh-CN"): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }
  return new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  }).format(date);
}

export function formatRelativeTime(value?: string | null, locale = "zh-CN"): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }
  const diffMs = date.getTime() - Date.now();
  const diffSec = Math.round(diffMs / 1000);
  const rtf = new Intl.RelativeTimeFormat(locale, { numeric: "auto" });
  const abs = Math.abs(diffSec);
  if (abs < 60) return rtf.format(diffSec, "second");
  const diffMin = Math.round(diffSec / 60);
  if (Math.abs(diffMin) < 60) return rtf.format(diffMin, "minute");
  const diffHour = Math.round(diffMin / 60);
  if (Math.abs(diffHour) < 24) return rtf.format(diffHour, "hour");
  const diffDay = Math.round(diffHour / 24);
  return rtf.format(diffDay, "day");
}

export function formatNumber(value: number | undefined, digits = 2, locale = "zh-CN"): string {
  if (value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return new Intl.NumberFormat(locale, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits
  }).format(value);
}

export function formatCurrency(value: number | undefined, locale = "zh-CN"): string {
  if (value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return new Intl.NumberFormat(locale, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 4,
    maximumFractionDigits: 4
  }).format(value);
}

export function truncate(value: string, max = 24): string {
  if (value.length <= max) {
    return value;
  }
  return `${value.slice(0, max)}...`;
}
