"use client";

import React, { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  fetchSubmissionDetail,
  SubmissionDetailResponse,
  RunRecord,
} from "../lib/api";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";
import { formatMicros, formatTPS, getScoreColor } from "../lib/utils";
import MetricBadge from "./MetricBadge";

interface SubmissionDrawerProps {
  submissionId: string | null;
  onClose: () => void;
}

export default function SubmissionDrawer({
  submissionId,
  onClose,
}: SubmissionDrawerProps) {
  const [detail, setDetail] = useState<SubmissionDetailResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [isClient, setIsClient] = useState(false);

  useEffect(() => {
    setIsClient(true);
  }, []);

  useEffect(() => {
    if (!submissionId) {
      setDetail(null);
      return;
    }

    let active = true;
    setIsLoading(true);
    setError(null);

    async function loadDetail() {
      try {
        const data = await fetchSubmissionDetail(submissionId!);
        if (active) {
          setDetail(data);
        }
      } catch (err: any) {
        if (active) {
          setError(err);
        }
      } finally {
        if (active) {
          setIsLoading(false);
        }
      }
    }

    loadDetail();

    return () => {
      active = false;
    };
  }, [submissionId]);

  // Handle ESC key press to close drawer
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  // Format timestamp for charts
  const formatChartDate = (timestampStr: string) => {
    try {
      const d = new Date(timestampStr);
      return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    } catch {
      return "";
    }
  };

  const chartData = detail?.runs
    ? [...detail.runs].reverse().map((r) => ({
        ...r,
        formattedTime: formatChartDate(r.timestamp),
        correctnessPct: r.success_rate * 100,
      }))
    : [];

  return (
    <AnimatePresence>
      {submissionId && (
        <>
          {/* Backdrop overlay */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 0.5 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
            className="fixed inset-0 z-50 bg-black backdrop-blur-xs cursor-pointer"
          />

          {/* Drawer body */}
          <motion.div
            initial={{ x: "100%" }}
            animate={{ x: 0 }}
            exit={{ x: "100%" }}
            transition={{ type: "tween", duration: 0.35, ease: "easeInOut" }}
            className="fixed right-0 top-0 bottom-0 z-50 w-full sm:w-[500px] md:w-[600px] lg:w-[650px] bg-background-secondary border-l border-border-default/80 shadow-2xl flex flex-col overflow-hidden"
          >
            {/* Header */}
            <div className="p-6 border-b border-border-default flex items-start justify-between bg-background-card/50">
              <div className="flex flex-col gap-1.5 max-w-[80%]">
                <span className="text-[10px] text-text-tertiary font-mono tracking-widest uppercase">
                  Engine Profile & Runs
                </span>
                <h2 className="text-base sm:text-lg font-bold text-white truncate font-mono select-all">
                  {submissionId}
                </h2>
                {detail && (
                  <div className="flex items-center gap-2 mt-1.5 flex-wrap">
                    <MetricBadge
                      label="Best Score"
                      value={detail.best_score.toFixed(1)}
                      color={detail.best_score >= 80 ? "green" : detail.best_score >= 50 ? "yellow" : "red"}
                    />
                    <MetricBadge
                      label="Best P99"
                      value={formatMicros(detail.best_p99)}
                      color="blue"
                    />
                    <MetricBadge
                      label="Total Runs"
                      value={detail.runs.length}
                    />
                  </div>
                )}
              </div>

              {/* Close Button */}
              <button
                onClick={onClose}
                className="p-1.5 rounded-lg border border-border-default hover:border-accent-red/40 hover:text-accent-red transition-all bg-background-secondary shadow-sm"
              >
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>

            {/* Scrollable Content */}
            <div className="flex-1 overflow-y-auto p-6 space-y-8 custom-scrollbar">
              {isLoading ? (
                <div className="h-64 flex flex-col items-center justify-center gap-2">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-accent-blue" />
                  <span className="text-xs text-text-secondary font-mono">
                    Querying run metrics database...
                  </span>
                </div>
              ) : error ? (
                <div className="border border-accent-red/20 rounded-xl bg-accent-red/5 p-4 text-center">
                  <span className="text-xs text-accent-red font-mono">
                    Error: {error.message || "Failed to load telemetry detail"}
                  </span>
                </div>
              ) : detail ? (
                <>
                  {/* Chart Section 1: Latency Trends */}
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <h3 className="text-xs font-bold text-text-secondary uppercase tracking-wider font-sans select-none">
                        Latency Profile (p50 / p90 / p99)
                      </h3>
                      <span className="text-[10px] text-text-tertiary font-mono">
                        Newest to left
                      </span>
                    </div>

                    <div className="h-[220px] w-full bg-background-card/45 border border-border-default rounded-xl p-3 shadow-inner">
                      {isClient && chartData.length > 0 ? (
                        <ResponsiveContainer width="100%" height="100%">
                          <LineChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#1a1a2e" />
                            <XAxis dataKey="formattedTime" stroke="#444466" fontSize={9} className="font-mono" />
                            <YAxis stroke="#444466" fontSize={9} className="font-mono" unit="µ" />
                            <Tooltip
                              contentStyle={{ background: "#0d0d1f", borderColor: "#2a2a4e", borderRadius: 8, fontSize: 11 }}
                              labelStyle={{ color: "#8888aa", fontFamily: "monospace" }}
                            />
                            <Legend wrapperStyle={{ fontSize: 10, marginTop: 5 }} />
                            <Line name="P50" type="monotone" dataKey="p50_micros" stroke="#00ff88" strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
                            <Line name="P90" type="monotone" dataKey="p90_micros" stroke="#ffaa00" strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
                            <Line name="P99" type="monotone" dataKey="p99_micros" stroke="#ff4444" strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
                          </LineChart>
                        </ResponsiveContainer>
                      ) : (
                        <div className="h-full flex items-center justify-center text-xs text-text-tertiary font-mono">
                          No chart data available
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Chart Section 2: Correctness Trend */}
                  <div className="space-y-3">
                    <h3 className="text-xs font-bold text-text-secondary uppercase tracking-wider font-sans select-none">
                      Correctness Trend (%)
                    </h3>

                    <div className="h-[180px] w-full bg-background-card/45 border border-border-default rounded-xl p-3 shadow-inner">
                      {isClient && chartData.length > 0 ? (
                        <ResponsiveContainer width="100%" height="100%">
                          <LineChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#1a1a2e" />
                            <XAxis dataKey="formattedTime" stroke="#444466" fontSize={9} className="font-mono" />
                            <YAxis stroke="#444466" fontSize={9} className="font-mono" domain={[0, 100]} unit="%" />
                            <Tooltip
                              contentStyle={{ background: "#0d0d1f", borderColor: "#2a2a4e", borderRadius: 8, fontSize: 11 }}
                              labelStyle={{ color: "#8888aa", fontFamily: "monospace" }}
                            />
                            <Line name="Success Rate" type="monotone" dataKey="correctnessPct" stroke="#0066ff" strokeWidth={2} dot={{ r: 3 }} />
                          </LineChart>
                        </ResponsiveContainer>
                      ) : (
                        <div className="h-full flex items-center justify-center text-xs text-text-tertiary font-mono">
                          No trend data available
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Run History List */}
                  <div className="space-y-3">
                    <h3 className="text-xs font-bold text-text-secondary uppercase tracking-wider font-sans select-none">
                      Execution Run History
                    </h3>

                    <div className="overflow-hidden rounded-xl border border-border-default/60 bg-background-card/25 shadow-lg">
                      <table className="w-full text-[11px] text-left border-collapse font-mono">
                        <thead>
                          <tr className="bg-background-secondary border-b border-border-default text-text-secondary uppercase font-bold text-[9px] select-none">
                            <th className="px-4 py-2.5">Run ID</th>
                            <th className="px-4 py-2.5">Score</th>
                            <th className="px-4 py-2.5">P99</th>
                            <th className="px-4 py-2.5">P50</th>
                            <th className="px-4 py-2.5">TPS</th>
                            <th className="px-4 py-2.5">Success</th>
                            <th className="px-4 py-2.5">Timestamp</th>
                          </tr>
                        </thead>
                        <tbody>
                          {detail.runs.map((run) => (
                            <tr
                              key={run.run_id}
                              className="border-b border-border-default/40 hover:bg-background-secondary/50 transition-colors"
                            >
                              <td className="px-4 py-2 text-white font-bold">
                                #{run.run_id}
                              </td>
                              <td className="px-4 py-2">
                                <span className={getScoreColor(run.score)}>
                                  {run.score.toFixed(1)}
                                </span>
                              </td>
                              <td className="px-4 py-2 text-white">
                                {formatMicros(run.p99_micros)}
                              </td>
                              <td className="px-4 py-2 text-text-secondary">
                                {formatMicros(run.p50_micros)}
                              </td>
                              <td className="px-4 py-2 text-accent-blue">
                                {formatTPS(run.tps)}
                              </td>
                              <td className="px-4 py-2 text-white">
                                {(run.success_rate * 100).toFixed(1)}%
                              </td>
                              <td className="px-4 py-2 text-text-tertiary text-[10px]">
                                {new Date(run.timestamp).toLocaleTimeString()}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </>
              ) : (
                <div className="h-64 flex items-center justify-center text-xs text-text-tertiary font-mono">
                  No submission profile selected
                </div>
              )}
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}
