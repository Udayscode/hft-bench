import { useEffect, useState, useRef } from "react";
import { fetchScores, ScoresResponse, ScoreRow } from "./api";

export interface RankChange {
  from: number;
  to: number;
}

export function useLeaderboard(pollIntervalMs = 1500) {
  const [data, setData] = useState<ScoresResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const [rankChanges, setRankChanges] = useState<Record<string, RankChange>>({});

  const prevScoresRef = useRef<ScoreRow[]>([]);

  useEffect(() => {
    let active = true;
    let timer: any = null;

    async function updateLeaderboard() {
      try {
        const response = await fetchScores();
        if (!active) return;

        const prevScores = prevScoresRef.current;
        if (prevScores.length > 0) {
          const changes: Record<string, RankChange> = {};
          const prevRankMap = new Map<string, number>();
          
          prevScores.forEach((row) => {
            prevRankMap.set(row.submission_id, row.rank);
          });

          response.scores.forEach((row) => {
            const oldRank = prevRankMap.get(row.submission_id);
            if (oldRank !== undefined && oldRank !== row.rank) {
              changes[row.submission_id] = { from: oldRank, to: row.rank };
            }
          });

          // Only update rankChanges state if there are actual updates to avoid redundant re-renders
          if (Object.keys(changes).length > 0) {
            setRankChanges(changes);
            // Clear the flashes after 3 seconds so they don't persist forever
            setTimeout(() => {
              if (active) setRankChanges({});
            }, 3000);
          }
        }

        prevScoresRef.current = response.scores;
        setData(response);
        setError(null);
      } catch (err: any) {
        if (!active) return;
        setError(err);
      } finally {
        if (active) {
          setIsLoading(false);
        }
      }
    }

    updateLeaderboard();
    timer = setInterval(updateLeaderboard, pollIntervalMs);

    return () => {
      active = false;
      if (timer) {
        clearInterval(timer);
      }
    };
  }, [pollIntervalMs]);

  return {
    data,
    rankChanges,
    isLoading,
    error,
    lastUpdated: data?.last_updated || null,
  };
}
