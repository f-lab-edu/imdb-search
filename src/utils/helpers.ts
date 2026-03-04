import { config } from "../config/index.js";

export const isPrimary = (fileType: string) =>
  fileType === "TITLE_BASICS" || fileType === "NAME_BASICS";

export const getDatasetInfoByFileName = (fileName: string) => {
  const fileConfig = config.datasets.files.find((f) =>
    fileName.endsWith(f.name),
  );

  if (!fileConfig) return null;

  return { type: fileConfig.type, isPrimary: isPrimary(fileConfig.type) };
};
