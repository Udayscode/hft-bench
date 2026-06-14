"use client";

import React, { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { formatMicros } from "../lib/utils";

interface StatsStripProps {
  totalSubmissions: number;
  bestP99: number;
  avgCorrectness: number;
  activeVMs: number;
}

interface StatCardProps {
  label: string;
  value: string | number;
  subValue?: string;
  icon: React.ReactNode;
  pulse?: boolean;
  trend?: "up" | "down" | "neutral";
  colorClass?: string;
}

function AnimatedNumber({ value }: { value: number | string }) {
  const [displayVal, setDisplayVal] = useState(value);

  useEffect(() => {
    setDisplayVal(value);
  }, [value]);

  return <span className="font-mono text-xl sm:text-2xl font-bold tracking-tight">{displayVal}</span>;
}

function StatCard({
  label,
  value,
  subValue,
  icon,
  pulse = false,
  trend,
  colorClass = "text-white",
}: StatCardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 15 }}
      animate={{ opacity: 1, y: 0 }}
      whileHover={{ y: -3, transition: { duration: 0.2 } }}
      className="relative overflow-hidden rounded-xl border border-border-default/80 bg-background-card/45 p-4 shadow-lg backdrop-blur-sm group shimmer-card"
    >
      <div className="flex items-start justify-between">
        <div>
          <p className="text-[11px] font-medium text-text-secondary uppercase tracking-wider">
            {label}
          </p>
          <div className={`mt-2 flex items-baseline gap-1.5 ${colorClass}`}>
            {typeof value === "number" ? <AnimatedNumber value={value} /> : <span className="font-mono text-xl sm:text-2xl font-bold tracking-tight">{value}</span>}
            {subValue && (
              <span className="text-[10px] text-text-tertiary font-mono">
                {subValue}
              </span>
            )}
          </div>
        </div>
        <div className="rounded-lg p-2 bg-background-secondary border border-border-accent/30 text-text-secondary group-hover:text-accent-blue group-hover:border-accent-blue/30 transition-colors">
          {icon}
        </div>
      </div>

      {trend && (
        <div className="mt-2.5 flex items-center gap-1">
          {trend === "up" && (
            <span className="text-[10px] text-accent-green flex items-center font-medium">
              <svg className="w-3.5 h-3.5 mr-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
              </svg>
              Performance trend rising
            </span>
          )}
          {trend === "down" && (
            <span className="text-[10px] text-accent-red flex items-center font-medium">
              <svg className="w-3.5 h-3.5 mr-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M13 17h8m0 0v-8m0 8l-8-8-4 4-6-6" />
              </svg>
              High latency detected
            </span>
          )}
        </div>
      )}

      {/* Decorative pulse glow */}
      {pulse && (
        <div className="absolute top-2 right-2 flex h-2 w-2">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-accent-green opacity-75"></span>
          <span className="relative inline-flex rounded-full h-2 w-2 bg-accent-green"></span>
        </div>
      )}
    </motion.div>
  );
}

export default function StatsStrip({
  totalSubmissions,
  bestP99,
  avgCorrectness,
  activeVMs,
}: StatsStripProps) {
  const formattedP99 = bestP99 > 0 ? formatMicros(bestP99) : "—";
  const p99Parts = formattedP99.split(" ");

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 w-full">
      {/* 1. Total Submissions */}
      <StatCard
        label="Total Submissions"
        value={totalSubmissions}
        trend={totalSubmissions > 0 ? "up" : undefined}
        icon={
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01" />
          </svg>
        }
      />

      {/* 2. Best Latency */}
      <StatCard
        label="Best P99 Latency"
        value={p99Parts[0]}
        subValue={p99Parts[1] || ""}
        colorClass="text-accent-green glow-green"
        icon={
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
        }
      />

      {/* 3. Average Correctness */}
      <StatCard
        label="Avg Correctness"
        value={(avgCorrectness * 100).toFixed(2)}
        subValue="%"
        colorClass={avgCorrectness >= 0.85 ? "text-accent-green" : avgCorrectness >= 0.6 ? "text-accent-yellow" : "text-accent-red"}
        icon={
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
        }
      />

      {/* 4. Active Benchmarks */}
      <StatCard
        label="Active Now"
        value={activeVMs}
        subValue="VMs"
        pulse={activeVMs > 0}
        colorClass={activeVMs > 0 ? "text-accent-blue glow-blue" : "text-text-secondary"}
        icon={
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
          </svg>
        }
      />
    </div>
  );
}
