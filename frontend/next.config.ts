import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // Evita el warning de múltiples lockfiles en monorepo
  outputFileTracingRoot: path.join(__dirname, "../"),
};

export default nextConfig;
