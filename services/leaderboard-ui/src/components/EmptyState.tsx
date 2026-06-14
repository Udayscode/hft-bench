"use client";

import React, { useState } from "react";

export default function EmptyState() {
  const [copied, setCopied] = useState(false);
  const curlCommand = `curl -X POST http://localhost:8080/submit \\
  -F "submission_id=my-hft-engine" \\
  -F "binary=@./path/to/my-engine-binary"`;

  const copyToClipboard = () => {
    navigator.clipboard.writeText(curlCommand);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="w-full flex flex-col items-center justify-center p-8 py-16 text-center select-none">
      
      {/* Circuit Board / Exchange Graph SVG */}
      <div className="relative w-24 h-24 mb-6 text-accent-blue/80 flex items-center justify-center bg-background-card/50 rounded-2xl border border-border-default/80 shadow-lg p-4 shimmer-card">
        <svg className="w-full h-full" viewBox="0 0 64 64" fill="none" stroke="currentColor" strokeWidth={2}>
          {/* Main chips & lines */}
          <rect x="22" y="22" width="20" height="20" rx="4" className="stroke-accent-blue" strokeWidth={2.5} />
          <path d="M32 6v16M32 42v16M6 32h16M42 32h16" strokeLinecap="round" />
          {/* Nodes */}
          <circle cx="32" cy="6" r="3" className="fill-accent-green stroke-none animate-pulseFast" />
          <circle cx="32" cy="58" r="3" className="fill-accent-green stroke-none" />
          <circle cx="6" cy="32" r="3" className="fill-accent-green stroke-none" />
          <circle cx="58" cy="32" r="3" className="fill-accent-green stroke-none animate-pulseFast" />
          
          {/* Inner connections */}
          <path d="M26 28h12M28 32h8M30 36h4" strokeWidth={1.5} strokeLinecap="round" />
        </svg>
      </div>

      <h2 className="text-base font-bold text-white tracking-tight">
        No Submissions Yet
      </h2>
      <p className="mt-1.5 text-xs text-text-secondary max-w-sm leading-relaxed">
        The benchmark metrics database is currently empty. Run your first high-frequency matching engine benchmark to populate this leaderboard.
      </p>

      <div className="mt-8 w-full max-w-md text-left space-y-2.5">
        <label className="text-[10px] text-text-secondary uppercase font-mono tracking-widest font-semibold block px-1 select-none">
          Submit your first engine:
        </label>
        
        <div className="relative rounded-xl border border-border-default/80 bg-background-primary/80 p-4 font-mono text-xs text-accent-green select-all shadow-inner overflow-x-auto">
          <pre className="pr-12 text-[11px] leading-relaxed">
            {curlCommand}
          </pre>

          {/* Copy Button */}
          <button
            onClick={copyToClipboard}
            className="absolute top-3.5 right-3.5 p-2 rounded-lg border border-border-default/60 hover:border-accent-blue/50 hover:bg-background-secondary/60 hover:text-accent-blue transition-all bg-background-card text-text-secondary cursor-pointer shadow-sm select-none"
            title="Copy command"
          >
            {copied ? (
              <svg className="w-4 h-4 text-accent-green" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
              </svg>
            ) : (
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3" />
              </svg>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
