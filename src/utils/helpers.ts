import { config } from "../config/index.js";
import { type DatasetKey } from "./types.js";

export const isPrimary = (fileType: string) =>
  fileType === "TITLE_BASICS" || fileType === "NAME_BASICS";

export const getDatasetInfoByFileName = (fileName: string) => {
  const fileConfig = config.datasets.files.find((f) =>
    fileName.endsWith(f.name),
  );

  if (!fileConfig) return null;

  return {
    type: fileConfig.type as DatasetKey,
    isPrimary: isPrimary(fileConfig.type),
  };
};
