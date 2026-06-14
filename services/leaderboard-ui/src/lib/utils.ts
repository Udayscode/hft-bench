export function formatMicros(micros: number): string {
  if (micros === 0) return "—";
  if (micros < 1000) {
    return `${micros} µs`;
  }
  const ms = micros / 1000;
  if (ms < 1000) {
    return `${ms.toFixed(2)} ms`;
  }
  return `${(ms / 1000).toFixed(2)} s`;
}

export function formatTPS(tps: number): string {
  if (!tps) return "0";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(tps);
}

export function getScoreColor(score: number): string {
  if (score >= 80) return "text-accent-green";
  if (score >= 50) return "text-accent-yellow";
  return "text-accent-red";
}

export function getScoreBg(score: number): string {
  if (score >= 80) return "bg-accent-green/10 text-accent-green border-accent-green/20";
  if (score >= 50) return "bg-accent-yellow/10 text-accent-yellow border-accent-yellow/20";
  return "bg-accent-red/10 text-accent-red border-accent-red/20";
}

export function getLatencyColor(p99: number): string {
  if (p99 === 0) return "text-text-secondary";
  if (p99 < 1000) return "text-accent-green"; // < 1ms
  if (p99 <= 3000) return "text-accent-yellow"; // 1ms - 3ms
  return "text-accent-red"; // > 3ms
}

export function getLatencyBg(p99: number): string {
  if (p99 === 0) return "bg-border-default/50 text-text-secondary border-border-default";
  if (p99 < 1000) return "bg-accent-green/10 text-accent-green border-accent-green/20";
  if (p99 <= 3000) return "bg-accent-yellow/10 text-accent-yellow border-accent-yellow/20";
  return "bg-accent-red/10 text-accent-red border-accent-red/20";
}

export function getCorrectnessColor(rate: number): string {
  const percentage = rate * 100;
  if (percentage >= 85) return "text-accent-green";
  if (percentage >= 60) return "text-accent-yellow";
  return "text-accent-red";
}

export function getCorrectnessBg(rate: number): string {
  const percentage = rate * 100;
  if (percentage >= 85) return "bg-accent-green/10 text-accent-green border-accent-green/20";
  if (percentage >= 60) return "bg-accent-yellow/10 text-accent-yellow border-accent-yellow/20";
  return "bg-accent-red/10 text-accent-red border-accent-red/20";
}

export function cn(...classes: (string | undefined | null | boolean)[]): string {
  return classes.filter(Boolean).join(" ");
}

export function getRelativeTime(timestampStr: string): string {
  if (!timestampStr) return "never";
  try {
    const date = new Date(timestampStr);
    if (isNaN(date.getTime())) return "—";
    
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffSecs = Math.floor(diffMs / 1000);
    
    if (diffSecs < 1) return "just now";
    if (diffSecs < 60) return `${diffSecs}s ago`;
    
    const diffMins = Math.floor(diffSecs / 60);
    if (diffMins < 60) return `${diffMins}m ago`;
    
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    
    return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  } catch (e) {
    return "—";
  }
}
