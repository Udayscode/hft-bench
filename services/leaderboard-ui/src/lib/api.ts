const BASE_URL = process.env.NEXT_PUBLIC_API_URL || "";

export interface ScoreRow {
  rank: number;
  submission_id: string;
  p50_micros: number;
  p90_micros: number;
  p99_micros: number;
  success_rate: number;
  score: number;
  total_orders: number;
  correctness_violations: number;
  tps: number;
  timestamp: string;
}

export interface ScoresResponse {
  scores: ScoreRow[];
  last_updated: string;
  active_benchmarks: number;
}

export interface RunRecord {
  run_id: number;
  p50_micros: number;
  p90_micros: number;
  p99_micros: number;
  success_rate: number;
  score: number;
  tps: number;
  timestamp: string;
}

export interface SubmissionDetailResponse {
  submission_id: string;
  runs: RunRecord[];
  best_score: number;
  best_p99: number;
}

export async function fetchScores(): Promise<ScoresResponse> {
  const url = `${BASE_URL}/api/scores`;
  const res = await fetch(url, {
    method: "GET",
    headers: {
      "Accept": "application/json",
    },
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch scores: status ${res.status}`);
  }
  return res.json();
}

export async function fetchSubmissionDetail(submissionId: string): Promise<SubmissionDetailResponse> {
  const url = `${BASE_URL}/api/scores/${encodeURIComponent(submissionId)}`;
  const res = await fetch(url, {
    method: "GET",
    headers: {
      "Accept": "application/json",
    },
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch submission detail: status ${res.status}`);
  }
  return res.json();
}
