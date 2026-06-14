import React from "react";

interface LiveIndicatorProps {
  className?: string;
  label?: string;
}

export default function LiveIndicator({ className = "", label = "LIVE" }: LiveIndicatorProps) {
  return (
    <div className={`flex items-center gap-2 select-none ${className}`}>
      <span className="relative flex h-2.5 w-2.5">
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-accent-green opacity-75"></span>
        <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-accent-green shadow-[0_0_8px_#00ff88]"></span>
      </span>
      <span className="text-[11px] font-bold font-mono tracking-wider text-accent-green uppercase">
        {label}
      </span>
    </div>
  );
}
