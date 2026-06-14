import React from "react";
import { cn } from "../lib/utils";

interface MetricBadgeProps {
  label: string;
  value: string | number;
  unit?: string;
  color?: "green" | "yellow" | "red" | "blue" | "neutral";
  className?: string;
}

export default function MetricBadge({
  label,
  value,
  unit = "",
  color = "neutral",
  className = "",
}: MetricBadgeProps) {
  const colorMap = {
    green: "bg-accent-green/10 text-accent-green border-accent-green/20",
    yellow: "bg-accent-yellow/10 text-accent-yellow border-accent-yellow/20",
    red: "bg-accent-red/10 text-accent-red border-accent-red/20",
    blue: "bg-accent-blue/10 text-accent-blue border-accent-blue/20",
    neutral: "bg-border-default/50 text-text-secondary border-border-default",
  };

  return (
    <div
      className={cn(
        "inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full border text-[11px] font-semibold select-none",
        colorMap[color],
        className
      )}
    >
      <span className="opacity-60 uppercase text-[9px] tracking-wider font-sans">
        {label}
      </span>
      <span className="font-mono font-medium">
        {value}
        {unit && <span className="text-[9px] opacity-70 ml-0.5">{unit}</span>}
      </span>
    </div>
  );
}
