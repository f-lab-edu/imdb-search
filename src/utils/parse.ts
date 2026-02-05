import fs, { type PathLike } from "node:fs";
import readline from "node:readline";
import type { DatasetType, TableName } from "./types.js";

export const toSnakeCase = (str: string) =>
  str.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);

export async function* generateTSVlines<T extends DatasetType>(
  filePath: PathLike,
): AsyncGenerator<T> {
  const fileStream = fs.createReadStream(filePath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: fileStream });

  try {
    const iterator = rl[Symbol.asyncIterator]();
    const firstLine = (await iterator.next()).value;

    if (!firstLine) throw new Error("failed to find headers");

    const headers = firstLine.split("\t").map((h) => toSnakeCase(h));

    for await (const line of iterator) {
      const fields = line.split("\t");
      const obj = headers.reduce(
        (acc, curr, idx) => {
          acc[curr] = fields[idx] == "\\N" || !fields[idx] ? null : fields[idx];
          return acc;
        },
        {} as Record<string, string | null>,
      );

      yield obj as unknown as T;
    }
  } finally {
    rl.close();
  }
}
