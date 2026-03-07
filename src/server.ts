import express, { type Request, type Response, type NextFunction } from "express";
import type { OpenSearchQuery, SearchParams } from "./db/opensearch/queries.js";
import type { MysqlQuery } from "./db/mysql/queries.js";

export function createApp(osQuery: OpenSearchQuery, mysqlQuery: MysqlQuery) {
  const app = express();

  app.get("/search", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const params: SearchParams = {};

      if (req.query.q) params.q = String(req.query.q);
      if (req.query.type) params.type = String(req.query.type);
      if (req.query.genre) params.genre = String(req.query.genre);
      if (req.query.yearFrom) params.yearFrom = Number(req.query.yearFrom);
      if (req.query.yearTo) params.yearTo = Number(req.query.yearTo);
      if (req.query.ratingMin) params.ratingMin = Number(req.query.ratingMin);
      if (req.query.ratingMax) params.ratingMax = Number(req.query.ratingMax);
      if (req.query.adult === "true") params.adult = true;
      if (req.query.sort) {
        const s = String(req.query.sort);
        if (s === "rating" || s === "year" || s === "votes" || s === "relevance") {
          params.sort = s;
        }
      }
      if (req.query.page) params.page = Number(req.query.page);
      if (req.query.size) params.size = Number(req.query.size);

      const result = await osQuery.searchTitles(params);
      res.json(result);
    } catch (err) {
      next(err);
    }
  });

  app.get("/titles/:tconst", async (req: Request<{ tconst: string }>, res: Response, next: NextFunction) => {
    try {
      const result = await mysqlQuery.getTitleDetail(req.params.tconst);
      if (!result) return res.status(404).json({ error: "not found" });
      res.json(result);
    } catch (err) {
      next(err);
    }
  });

  app.get("/persons/:nconst", async (req: Request<{ nconst: string }>, res: Response, next: NextFunction) => {
    try {
      const result = await mysqlQuery.getPersonDetail(req.params.nconst);
      if (!result) return res.status(404).json({ error: "not found" });
      res.json(result);
    } catch (err) {
      next(err);
    }
  });

  app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
    console.error(err);
    res.status(500).json({ error: err.message });
  });

  return app;
}
