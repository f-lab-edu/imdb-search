import { config } from "../config/index.js";

export const getDatasetInfoByFileName = (fileName: string) => {
  const fileConfig = config.datasets.files.find((f) =>
    fileName.endsWith(f.name),
  );

  if (!fileConfig) return null;

  const isPrimary =
    fileConfig.type === "TITLE_BASICS" || fileConfig.type === "NAME_BASICS";

  return { type: fileConfig.type, isPrimary };
};
