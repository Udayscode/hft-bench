"use client";

import React, { useEffect, useState } from "react";
import LiveIndicator from "./LiveIndicator";
import { getRelativeTime } from "../lib/utils";

interface HeaderProps {
  lastUpdated: string | null;
  activeBenchmarks?: number;
}

export default function Header({ lastUpdated, activeBenchmarks = 0 }: HeaderProps) {
  const [relativeTime, setRelativeTime] = useState("just now");

  useEffect(() => {
    if (!lastUpdated) return;

    const interval = setInterval(() => {
      setRelativeTime(getRelativeTime(lastUpdated));
    }, 1000);

    setRelativeTime(getRelativeTime(lastUpdated));

    return () => clearInterval(interval);
  }, [lastUpdated]);

  return (
    <header className="sticky top-0 z-40 w-full bg-background-primary/80 backdrop-blur-md border-b border-border-default/50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
        
        {/* Left: Brand logo */}
        <div className="flex items-center gap-3">
          <div className="relative flex items-center justify-center w-8 h-8 rounded-lg bg-gradient-to-br from-accent-blue to-accent-green p-[1px] shadow-[0_0_15px_rgba(0,102,255,0.25)]">
            <div className="w-full h-full bg-background-secondary rounded-[7px] flex items-center justify-center">
              <svg
                className="w-4 h-4 text-accent-green"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                strokeWidth={2.5}
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z"
                />
              </svg>
            </div>
          </div>
          <div>
            <h1 className="text-sm sm:text-base font-bold text-white tracking-tight leading-none">
              HFT Benchmark Platform
            </h1>
            <span className="text-[10px] text-text-secondary font-mono tracking-wider">
              SANDBOX-TELEMETRY
            </span>
          </div>
        </div>

        {/* Center: Live Status */}
        <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-full bg-background-secondary border border-border-accent/40 shadow-inner">
          <LiveIndicator />
          <span className="h-3 w-[1px] bg-border-accent/60"></span>
          <span className="text-xs font-semibold text-text-secondary select-none">
            Live Stream Active
          </span>
        </div>

        {/* Right: Telemetry metadata */}
        <div className="flex items-center gap-4 text-right">
          <div className="flex flex-col">
            <span className="text-[10px] text-text-tertiary font-mono uppercase tracking-wider leading-none">
              Last Synced
            </span>
            <span className="text-xs font-semibold text-text-secondary font-mono">
              {lastUpdated ? relativeTime : "Waiting..."}
            </span>
          </div>

          <div className="flex items-center gap-2.5 px-3 py-1.5 rounded-lg bg-background-secondary/60 border border-border-default">
            <div className="flex flex-col text-left">
              <span className="text-[9px] text-text-tertiary uppercase font-mono leading-none">
                Active VMs
              </span>
              <span className="text-xs font-bold text-accent-blue font-mono leading-none mt-1">
                {activeBenchmarks}
              </span>
            </div>
            {activeBenchmarks > 0 && (
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-accent-blue opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-accent-blue"></span>
              </span>
            )}
          </div>
        </div>
      </div>
      
      {/* Bottom border gradient line */}
      <div className="h-[1px] w-full bg-gradient-to-r from-accent-green via-accent-blue to-transparent opacity-50"></div>
    </header>
  );
}
