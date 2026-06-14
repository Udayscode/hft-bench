"use client";

import React, { useState } from "react";
import Header from "../components/Header";
import StatsStrip from "../components/StatsStrip";
import Podium from "../components/Podium";
import LeaderboardTable from "../components/LeaderboardTable";
import SubmissionDrawer from "../components/SubmissionDrawer";
import EmptyState from "../components/EmptyState";
import { useLeaderboard } from "../lib/useLeaderboard";

// Loading Skeleton Component
function LoadingSkeleton() {
  return (
    <div className="space-y-8 animate-pulse select-none">
      {/* Stats Strip skeleton */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {[...Array(4)].map((_, i) => (
          <div key={i} className="h-24 rounded-xl border border-border-default/50 bg-background-card/45 p-4" />
        ))}
      </div>

      {/* Podium skeleton */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-6 items-end">
        <div className="h-[230px] rounded-2xl border border-border-default/50 bg-background-card/40" />
        <div className="h-[270px] rounded-2xl border border-border-default/50 bg-background-card/40" />
        <div className="h-[230px] rounded-2xl border border-border-default/50 bg-background-card/40" />
      </div>

      {/* Table skeleton */}
      <div className="space-y-4">
        <div className="h-6 w-48 bg-border-default rounded" />
        <div className="rounded-xl border border-border-default bg-background-card/25 h-72 w-full overflow-hidden p-4 space-y-4">
          <div className="grid grid-cols-10 gap-4 border-b border-border-default pb-3">
            {[...Array(10)].map((_, i) => (
              <div key={i} className="h-3 bg-border-default rounded" />
            ))}
          </div>
          {[...Array(5)].map((_, index) => (
            <div key={index} className="grid grid-cols-10 gap-4 py-2 border-b border-border-default/30">
              <div className="h-4 bg-border-default rounded col-span-1" />
              <div className="h-4 bg-border-default rounded col-span-2" />
              <div className="h-4 bg-border-default rounded col-span-1" />
              <div className="h-4 bg-border-default rounded col-span-1" />
              <div className="h-4 bg-border-default rounded col-span-1" />
              <div className="h-4 bg-border-default rounded col-span-1" />
              <div className="h-4 bg-border-default rounded col-span-1" />
              <div className="h-4 bg-border-default rounded col-span-1" />
              <div className="h-4 bg-border-default rounded col-span-1" />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default function Home() {
  const { data, rankChanges, isLoading, error, lastUpdated } = useLeaderboard(1500);
  const [selectedSubmissionId, setSelectedSubmissionId] = useState<string | null>(null);

  // Compute stat metrics from scores
  const scores = data?.scores || [];
  const totalSubmissions = scores.length;
  
  // Find best p99 (minimum non-zero value)
  const nonZeroP99s = scores.map((s) => s.p99_micros).filter((val) => val > 0);
  const bestP99 = nonZeroP99s.length > 0 ? Math.min(...nonZeroP99s) : 0;

  // Calculate average correctness
  const avgCorrectness =
    scores.length > 0
      ? scores.reduce((sum, s) => sum + s.success_rate, 0) / scores.length
      : 0;

  const activeVMs = data?.active_benchmarks || 0;

  return (
    <div className="min-h-screen bg-background-primary text-white flex flex-col font-sans">
      
      {/* Top Header */}
      <Header lastUpdated={lastUpdated} activeBenchmarks={activeVMs} />

      {/* Main dashboard content container */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-8 flex flex-col gap-8">
        
        {/* Connection Error Banner */}
        {error && (
          <div className="w-full flex items-center justify-between gap-3 px-4 py-3 rounded-lg bg-accent-red/10 border border-accent-red/20 text-accent-red font-mono text-xs select-none">
            <div className="flex items-center gap-2">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-accent-red opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-accent-red"></span>
              </span>
              <span>Sync degraded. Attempting reconnection to Go API telemetry service...</span>
            </div>
            <span className="opacity-75">{error.message || "Network Error"}</span>
          </div>
        )}

        {isLoading ? (
          <LoadingSkeleton />
        ) : scores.length === 0 ? (
          <EmptyState />
        ) : (
          <>
            {/* Stats Overview */}
            <StatsStrip
              totalSubmissions={totalSubmissions}
              bestP99={bestP99}
              avgCorrectness={avgCorrectness}
              activeVMs={activeVMs}
            />

            {/* Podium (Top 3) */}
            <Podium
              topScores={scores.slice(0, 3)}
              onSelectSubmission={setSelectedSubmissionId}
            />

            {/* Leaderboard Table (full listing) */}
            <LeaderboardTable
              scores={scores}
              rankChanges={rankChanges}
              onSelectSubmission={setSelectedSubmissionId}
            />
          </>
        )}
      </main>

      {/* Side Drill-Down Drawer */}
      <SubmissionDrawer
        submissionId={selectedSubmissionId}
        onClose={() => setSelectedSubmissionId(null)}
      />

      {/* Footer */}
      <footer className="w-full py-6 border-t border-border-default/50 bg-background-secondary/30 text-center select-none mt-auto">
        <div className="max-w-7xl mx-auto px-4 flex flex-col sm:flex-row items-center justify-between gap-3 text-text-tertiary font-mono text-[10px] uppercase tracking-wider">
          <span>HFT Sandbox Environment v1.4.0</span>
          <span>© {new Date().getFullYear()} Antigravity Execution Labs</span>
          <span>High-Frequency Telemetry Ingester</span>
        </div>
      </footer>
    </div>
  );
}
