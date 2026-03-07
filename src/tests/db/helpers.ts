import mysql from "mysql2/promise";

export const resetMysql = async (pool: mysql.Pool) => {
  if (!process.env.DB_NAME?.includes("test")) {
    throw new Error("resetMysql: refusing to run against non-test DB");
  }

  let conn = null;

  try {
    conn = await pool.getConnection();

    await conn.query("SET FOREIGN_KEY_CHECKS = 0");
    await conn.query(
      "DROP TABLE IF EXISTS TASKS, TITLE_GENRES, TITLE_AKAS, RATINGS, EPISODES, TITLE_PRINCIPALS, TITLE_CREW, TITLES, PERSONS, GENRES;"
    );
    await conn.query("SET FOREIGN_KEY_CHECKS = 1");

    console.log("reset db ok");
  } catch (err) {
    console.error(`failed to reset db: ${(err as Error).message}`);
  } finally {
    conn?.release();
  }
};
