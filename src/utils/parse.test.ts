import dotenv from "dotenv";
import path from "node:path";
import { generateTSVlines } from "./parse.js";
import type { NameBasics } from "./types.js";

dotenv.config({ path: ".env.dev" });

const testFile = "name.basics.tsv";
const testFilePath = path.join(
  process.env.HOME!,
  "Downloads",
  "test-data",
  testFile,
);

(async () => {
  let lineCnt = 0;
  for await (const lineObj of generateTSVlines<NameBasics>(testFilePath)) {
    console.log(lineObj);
    lineCnt++;

    if (lineCnt > 10) break;
  }
})();
