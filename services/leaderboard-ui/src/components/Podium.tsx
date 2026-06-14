"use client";

import React from "react";
import { motion, Variants } from "framer-motion";
import { ScoreRow } from "../lib/api";
import { formatMicros, getScoreColor } from "../lib/utils";

interface PodiumProps {
  topScores: ScoreRow[];
  onSelectSubmission: (id: string) => void;
}

interface PodiumCardProps {
  scoreRow: ScoreRow;
  rank: 1 | 2 | 3;
  onSelect: (id: string) => void;
}

function CircularProgress({ percent, colorClass }: { percent: number; colorClass: string }) {
  const radius = 22;
  const circumference = 2 * Math.PI * radius;
  const strokeDashoffset = circumference - (percent / 100) * circumference;

  return (
    <div className="relative flex items-center justify-center w-14 h-14 select-none">
      <svg className="w-full h-full transform -rotate-90">
        {/* Background track */}
        <circle
          cx="28"
          cy="28"
          r={radius}
          className="stroke-border-default fill-none"
          strokeWidth="3.5"
        />
        {/* Progress path */}
        <motion.circle
          cx="28"
          cy="28"
          r={radius}
          className={`fill-none stroke-current ${colorClass}`}
          strokeWidth="3.5"
          strokeDasharray={circumference}
          initial={{ strokeDashoffset: circumference }}
          animate={{ strokeDashoffset }}
          transition={{ duration: 1.2, ease: "easeOut" }}
          strokeLinecap="round"
        />
      </svg>
      <div className="absolute flex flex-col items-center">
        <span className="text-[10px] font-bold font-mono text-white leading-none">
          {Math.round(percent)}
        </span>
        <span className="text-[7px] text-text-secondary leading-none uppercase mt-0.5">OK</span>
      </div>
    </div>
  );
}

function PodiumCard({ scoreRow, rank, onSelect }: PodiumCardProps) {
  const isGold = rank === 1;
  const isSilver = rank === 2;
  const isBronze = rank === 3;

  const medalColor = isGold
    ? "bg-gradient-to-r from-[#ffe066] to-[#ffd700] shadow-[0_0_20px_rgba(255,215,0,0.35)] text-background-primary"
    : isSilver
    ? "bg-gradient-to-r from-[#e0e0e0] to-[#c0c0c0] shadow-[0_0_15px_rgba(192,192,192,0.25)] text-background-primary"
    : "bg-gradient-to-r from-[#d7a15c] to-[#cd7f32] shadow-[0_0_15px_rgba(205,127,50,0.25)] text-background-primary";

  const borderColor = isGold
    ? "border-rank-gold/50 shadow-[0_0_30px_rgba(255,215,0,0.08)]"
    : isSilver
    ? "border-rank-silver/30"
    : "border-rank-bronze/30";

  const entranceVariants: Variants = {
    hidden: { opacity: 0, y: 50, scale: 0.95 },
    visible: {
      opacity: 1,
      y: 0,
      scale: 1,
      transition: {
        type: "spring",
        stiffness: 100,
        damping: 15,
        delay: isGold ? 0.1 : isSilver ? 0.2 : 0.3,
      },
    },
  };

  const correctnessPct = (scoreRow.success_rate || 0) * 100;
  const scoreColor = getScoreColor(scoreRow.score);

  return (
    <motion.div
      variants={entranceVariants}
      initial="hidden"
      animate="visible"
      whileHover={{ y: -6, transition: { duration: 0.2 } }}
      onClick={() => onSelect(scoreRow.submission_id)}
      className={`relative w-full flex flex-col items-center rounded-2xl border ${borderColor} bg-gradient-to-b from-background-card to-background-secondary/90 p-5 cursor-pointer select-none shadow-xl group ${
        isGold ? "h-[290px] sm:h-[310px] shimmer-card" : "h-[250px] sm:h-[270px]"
      }`}
    >
      {/* Position Rank Badge */}
      <div className={`absolute -top-3.5 left-1/2 transform -translate-x-1/2 px-3 py-0.5 rounded-full text-xs font-bold font-mono tracking-wider ${medalColor}`}>
        RANK #{rank}
      </div>

      <div className="flex flex-col items-center justify-between h-full w-full mt-2">
        {/* Header: Submission info */}
        <div className="text-center w-full">
          <h3
            className="text-sm font-semibold text-white truncate max-w-full group-hover:text-accent-blue transition-colors px-1"
            title={scoreRow.submission_id}
          >
            {scoreRow.submission_id.length > 18
              ? `${scoreRow.submission_id.substring(0, 7)}...${scoreRow.submission_id.substring(scoreRow.submission_id.length - 7)}`
              : scoreRow.submission_id}
          </h3>
          <span className="text-[9px] text-text-tertiary font-mono tracking-wide mt-1 block">
            ID: {scoreRow.submission_id.substring(0, 8)}
          </span>
        </div>

        {/* Center: Large Score */}
        <div className="flex flex-col items-center select-none">
          <span className="text-[10px] text-text-secondary uppercase font-mono tracking-widest leading-none">
            Composite Score
          </span>
          <span className={`text-3xl sm:text-4xl font-extrabold font-mono mt-1 ${scoreColor} tracking-tighter`}>
            {scoreRow.score.toFixed(1)}
          </span>
        </div>

        {/* Footer info: Latency Pill + Correctness Circular Chart */}
        <div className="flex items-center justify-between w-full border-t border-border-default/60 pt-4 px-2">
          {/* Latency metric */}
          <div className="flex flex-col text-left">
            <span className="text-[9px] text-text-tertiary font-mono uppercase tracking-wider leading-none">
              P99 Latency
            </span>
            <span className="text-sm font-bold font-mono text-white mt-1 leading-none">
              {formatMicros(scoreRow.p99_micros)}
            </span>
            <span className="text-[8px] text-text-secondary font-mono mt-0.5 leading-none">
              P50: {formatMicros(scoreRow.p50_micros)}
            </span>
          </div>

          {/* Correctness Progress */}
          <CircularProgress
            percent={correctnessPct}
            colorClass={correctnessPct >= 85 ? "text-accent-green" : correctnessPct >= 60 ? "text-accent-yellow" : "text-accent-red"}
          />
        </div>
      </div>
    </motion.div>
  );
}

export default function Podium({ topScores, onSelectSubmission }: PodiumProps) {
  // Ensure we have exactly 3 spots filled out with placeholders if needed
  const podiumData: (ScoreRow | null)[] = [null, null, null];
  
  if (topScores[0]) podiumData[0] = topScores[0]; // #1
  if (topScores[1]) podiumData[1] = topScores[1]; // #2
  if (topScores[2]) podiumData[2] = topScores[2]; // #3

  // Return empty if there are no submissions at all
  if (topScores.length === 0) return null;

  return (
    <div className="w-full flex flex-col gap-4 mt-2">
      <div className="flex items-center gap-2 px-1">
        <svg className="w-4 h-4 text-accent-yellow" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
        </svg>
        <h2 className="text-xs font-bold uppercase tracking-wider text-text-secondary select-none">
          Podium Leaders
        </h2>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-6 items-end w-full">
        {/* Layout Order: Rank 2 (Left), Rank 1 (Center), Rank 3 (Right) */}
        {/* Rank 2 (Silver) */}
        <div className="order-2 sm:order-1">
          {podiumData[1] ? (
            <PodiumCard scoreRow={podiumData[1]} rank={2} onSelect={onSelectSubmission} />
          ) : (
            <div className="rounded-2xl border border-border-default/50 bg-background-card/20 h-[220px] flex items-center justify-center border-dashed">
              <span className="text-xs text-text-tertiary uppercase font-mono tracking-widest">
                No Rank #2 Yet
              </span>
            </div>
          )}
        </div>

        {/* Rank 1 (Gold) */}
        <div className="order-1 sm:order-2">
          {podiumData[0] ? (
            <PodiumCard scoreRow={podiumData[0]} rank={1} onSelect={onSelectSubmission} />
          ) : (
            <div className="rounded-2xl border border-border-default/50 bg-background-card/20 h-[260px] flex items-center justify-center border-dashed">
              <span className="text-xs text-text-tertiary uppercase font-mono tracking-widest text-center">
                Submit Strategy<br />To Claim Top Spot
              </span>
            </div>
          )}
        </div>

        {/* Rank 3 (Bronze) */}
        <div className="order-3">
          {podiumData[2] ? (
            <PodiumCard scoreRow={podiumData[2]} rank={3} onSelect={onSelectSubmission} />
          ) : (
            <div className="rounded-2xl border border-border-default/50 bg-background-card/20 h-[220px] flex items-center justify-center border-dashed">
              <span className="text-xs text-text-tertiary uppercase font-mono tracking-widest">
                No Rank #3 Yet
              </span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
