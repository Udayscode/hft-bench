"use client";

import React, { useState, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { ScoreRow } from "../lib/api";
import { RankChange } from "../lib/useLeaderboard";
import {
  formatMicros,
  formatTPS,
  getScoreColor,
  getLatencyColor,
  getCorrectnessColor,
  getRelativeTime,
  cn
} from "../lib/utils";

interface LeaderboardTableProps {
  scores: ScoreRow[];
  rankChanges: Record<string, RankChange>;
  onSelectSubmission: (id: string) => void;
}

interface TableRowProps {
  row: ScoreRow;
  rankChange?: RankChange;
  onClick: () => void;
}

function TableRow({ row, rankChange, onClick }: TableRowProps) {
  const [flash, setFlash] = useState<"improved" | "dropped" | null>(null);

  useEffect(() => {
    if (rankChange) {
      if (rankChange.to < rankChange.from) {
        setFlash("improved");
      } else {
        setFlash("dropped");
      }
      
      const timer = setTimeout(() => {
        setFlash(null);
      }, 1500);
      
      return () => clearTimeout(timer);
    }
  }, [rankChange]);

  const rankColors = {
    1: "bg-rank-gold/25 text-rank-gold border-rank-gold/40 shadow-[0_0_8px_rgba(255,215,0,0.15)]",
    2: "bg-rank-silver/20 text-rank-silver border-rank-silver/30",
    3: "bg-rank-bronze/20 text-rank-bronze border-rank-bronze/35",
  };

  const getRankBadgeClass = (rank: number) => {
    if (rank === 1 || rank === 2 || rank === 3) {
      return rankColors[rank as 1 | 2 | 3];
    }
    return "bg-border-default/60 text-text-secondary border-border-default";
  };

  const correctnessPct = row.success_rate * 100;

  // Row flash background
  const getFlashBg = () => {
    if (flash === "improved") return "bg-accent-green/10 border-accent-green/30";
    if (flash === "dropped") return "bg-accent-red/10 border-accent-red/30";
    return "hover:bg-background-secondary/55 border-border-default/40";
  };

  return (
    <motion.tr
      layout
      onClick={onClick}
      className={cn(
        "cursor-pointer border-b transition-colors duration-300 select-none",
        getFlashBg()
      )}
      transition={{ type: "spring", stiffness: 300, damping: 30 }}
    >
      {/* RANK */}
      <td className="px-6 py-4 font-medium text-left">
        <span className={cn(
          "inline-flex items-center justify-center w-6 h-6 rounded-md border text-xs font-bold font-mono",
          getRankBadgeClass(row.rank)
        )}>
          {row.rank}
        </span>
      </td>

      {/* SUBMISSION ID */}
      <td className="px-6 py-4 font-medium text-white max-w-[180px] truncate">
        <div className="flex flex-col">
          <span className="font-mono text-sm tracking-tight text-white group-hover:text-accent-blue transition-colors">
            {row.submission_id}
          </span>
          <span className="text-[9px] text-text-tertiary font-mono">
            MD5: {row.submission_id.substring(0, 8)}
          </span>
        </div>
      </td>

      {/* SCORE */}
      <td className="px-6 py-4 font-mono">
        <div className="flex flex-col justify-center gap-1">
          <span className={cn("text-base font-extrabold", getScoreColor(row.score))}>
            {row.score.toFixed(2)}
          </span>
          <div className="w-16 h-1 bg-border-default rounded-full overflow-hidden">
            <div
              className={cn(
                "h-full rounded-full",
                row.score >= 80 ? "bg-accent-green" : row.score >= 50 ? "bg-accent-yellow" : "bg-accent-red"
              )}
              style={{ width: `${Math.min(row.score, 100)}%` }}
            />
          </div>
        </div>
      </td>

      {/* P99 */}
      <td className="px-6 py-4 font-mono font-semibold">
        <span className={getLatencyColor(row.p99_micros)}>
          {formatMicros(row.p99_micros)}
        </span>
      </td>

      {/* P90 */}
      <td className="px-6 py-4 font-mono font-medium text-text-secondary">
        <span className={getLatencyColor(row.p90_micros)}>
          {formatMicros(row.p90_micros)}
        </span>
      </td>

      {/* P50 */}
      <td className="px-6 py-4 font-mono text-text-secondary">
        <span className={getLatencyColor(row.p50_micros)}>
          {formatMicros(row.p50_micros)}
        </span>
      </td>

      {/* TPS */}
      <td className="px-6 py-4 font-mono font-semibold text-accent-blue">
        {formatTPS(row.tps)}
      </td>

      {/* CORRECTNESS */}
      <td className="px-6 py-4">
        <div className="flex flex-col justify-center gap-1 w-28">
          <div className="flex items-center justify-between text-[10px] font-mono font-semibold">
            <span className={getCorrectnessColor(row.success_rate)}>
              {correctnessPct.toFixed(2)}%
            </span>
            {row.correctness_violations > 0 && (
              <span className="text-accent-red text-[8px] px-1 bg-accent-red/10 rounded">
                {row.correctness_violations} err
              </span>
            )}
          </div>
          <div className="w-full h-1.5 bg-border-default rounded-full overflow-hidden">
            <div
              className={cn(
                "h-full rounded-full",
                correctnessPct >= 85 ? "bg-accent-green" : correctnessPct >= 60 ? "bg-accent-yellow" : "bg-accent-red"
              )}
              style={{ width: `${correctnessPct}%` }}
            />
          </div>
        </div>
      </td>

      {/* ORDERS */}
      <td className="px-6 py-4 font-mono text-text-secondary text-sm">
        {row.total_orders.toLocaleString()}
      </td>

      {/* TIME */}
      <td className="px-6 py-4 font-mono text-text-tertiary text-xs">
        {getRelativeTime(row.timestamp)}
      </td>
    </motion.tr>
  );
}

export default function LeaderboardTable({
  scores,
  rankChanges,
  onSelectSubmission
}: LeaderboardTableProps) {
  const [search, setSearch] = useState("");
  const [filteredScores, setFilteredScores] = useState<ScoreRow[]>(scores);

  useEffect(() => {
    if (!search.trim()) {
      setFilteredScores(scores);
    } else {
      const q = search.toLowerCase();
      setFilteredScores(
        scores.filter((s) => s.submission_id.toLowerCase().includes(q))
      );
    }
  }, [search, scores]);

  return (
    <div className="w-full flex flex-col gap-4">
      {/* Search and Filters Strip */}
      <div className="flex flex-col sm:flex-row items-center justify-between gap-3 px-1">
        <div className="flex items-center gap-2">
          <svg className="w-4 h-4 text-text-secondary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 12h16M4 18h7" />
          </svg>
          <h2 className="text-xs font-bold uppercase tracking-wider text-text-secondary select-none">
            Strategy Engine Leaderboard
          </h2>
        </div>

        <div className="relative w-full sm:w-72">
          <input
            type="text"
            placeholder="Search submission ID..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full bg-background-card/45 border border-border-default rounded-lg py-1.5 pl-9 pr-4 text-xs text-white placeholder-text-tertiary focus:outline-none focus:border-accent-blue/60 transition-all font-mono shadow-inner"
          />
          <svg
            className="absolute left-3 top-2.5 w-3.5 h-3.5 text-text-tertiary"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2.5}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        </div>
      </div>

      {/* Main Table */}
      <div className="w-full overflow-x-auto rounded-xl border border-border-default/60 bg-background-card/25 shadow-xl backdrop-blur-sm">
        <table className="w-full border-collapse text-left text-xs min-w-[900px] table-auto">
          <thead>
            <tr className="border-b border-border-default/80 bg-background-secondary/40 text-[10px] font-bold text-text-secondary uppercase tracking-wider select-none">
              <th className="px-6 py-4">Rank</th>
              <th className="px-6 py-4">Submission ID</th>
              <th className="px-6 py-4">Score</th>
              <th className="px-6 py-4">P99</th>
              <th className="px-6 py-4">P90</th>
              <th className="px-6 py-4">P50</th>
              <th className="px-6 py-4">TPS</th>
              <th className="px-6 py-4">Correctness</th>
              <th className="px-6 py-4">Orders</th>
              <th className="px-6 py-4">Sync Time</th>
            </tr>
          </thead>
          <tbody>
            <AnimatePresence initial={false}>
              {filteredScores.map((row) => (
                <TableRow
                  key={row.submission_id}
                  row={row}
                  rankChange={rankChanges[row.submission_id]}
                  onClick={() => onSelectSubmission(row.submission_id)}
                />
              ))}
            </AnimatePresence>
            {filteredScores.length === 0 && (
              <tr>
                <td colSpan={10} className="px-6 py-12 text-center text-text-tertiary font-mono">
                  No submissions matching search query
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
