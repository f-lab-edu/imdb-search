#!/usr/bin/env node
import "dotenv/config";
import { config } from "./config/index.js";
import { startPipeline } from "./pipeline.js";
import { startIndexer } from "./indexer.js";
import { startApi } from "./api.js";
import { startCron } from "./cron.js";

const [, , command, ...args] = process.argv;
const hasFlag = (flag: string) => args.includes(flag);

switch (command) {
  case "pipeline":
    await startPipeline(config, {
      skipDownload: hasFlag("--skip-download"),
      skipLoadTSV: hasFlag("--skip-load-tsv"),
      skipNormalization: hasFlag("--skip-normalization"),
      skipIntegrityCheck: hasFlag("--skip-integrity-check"),
    });
    break;

  case "index":
    await startIndexer(config);
    break;

  case "api":
    await startApi(config);
    break;

  case "run":
    await startPipeline(config);
    await startIndexer(config);
    break;

  case "cron":
    startCron(config);
    break;

  default:
    console.log(
      [
        "Usage: imdbs <command> [options]",
        "",
        "Commands:",
        "  pipeline                        Run data ingestion pipeline",
        "  pipeline --skip-download        Skip download phase",
        "  pipeline --skip-load-tsv        Skip TSV load phase",
        "  pipeline --skip-normalization   Skip normalization phase",
        "  pipeline --skip-integrity-check Skip integrity check",
        "  index                           Run OpenSearch indexer",
        "  api                        Start API server",
        "  run                        Run pipeline + index",
        "  cron                       Start cron daemon (daily 2am)",
      ].join("\n"),
    );

    process.exit(command ? 1 : 0);
}
