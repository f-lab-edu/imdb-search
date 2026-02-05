import axios from "axios";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import path from "node:path";
import { type Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGunzip } from "node:zlib";

export const downloadFile = async (
  dest: string,
  url: string,
): Promise<string> => {
  const outPath = dest.replace(".gz", "");
  const writer = fs.createWriteStream(outPath);

  try {
    await Promise.all([verifyFileUrl(url), verifyFileDest(outPath)]);

    const resp = await axios.get<Readable>(url, { responseType: "stream" });

    await pipeline(resp.data, createGunzip(), writer);

    return outPath;
  } catch (error) {
    writer.destroy();

    if (axios.isAxiosError(error)) {
      console.error(`Axios error: ${error.message}`);
    } else {
      console.error(`Download error: ${(error as Error).message}`);
    }

    // 파일이 아직 안 만들어진 경우 에러나는거 무시하기 위해 catch 붙임
    if (fs.existsSync(outPath))
      await fsPromises.unlink(outPath).catch(() => {});

    throw error;
  }
};

const verifyFileUrl = async (url: string) => {
  const validUrl = new URL(url);

  if (validUrl.protocol != "http:" && validUrl.protocol != "https:")
    throw new Error("only HTTP/HTTPS protocols are allowed");

  // TODO: check if url exists in url file list
};

const verifyFileDest = async (dest: string) => {
  const dir = path.dirname(dest);

  try {
    await fsPromises.access(dir);
  } catch (error) {
    await fsPromises.mkdir(dir, { recursive: true });
  }
};
