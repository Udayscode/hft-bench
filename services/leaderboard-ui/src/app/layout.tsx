import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "HFT Benchmark Platform — Live Leaderboard",
  description: "Real-time leaderboard & telemetry dashboard for High-Frequency Trading strategy engines.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body
        className="font-sans bg-background-primary text-white antialiased"
      >
        {children}
      </body>
    </html>
  );
}
