import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import matplotlib.pyplot as plt
import pandas as pd
import requests
from bs4 import BeautifulSoup


class DataCollectionPipeline:
    def __init__(self, db_path="market_intelligence.db", log_path="pipeline.log"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")

        self.conn.row_factory = sqlite3.Row
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "BookMarketIntelligence/1.0 (Educational)"}
        )


        self.logger = logging.getLogger("DataCollectionPipeline")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fh = logging.FileHandler(log_path, encoding="utf-8")
            fh.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            self.logger.addHandler(fh)
            self.logger.addHandler(logging.StreamHandler())


        self._create_tables()
        self.source_ids = self._seed_sources()
        self.logger.info("pipeline get initialized with DB: %s", self.db_path)


    def _create_tables(self):
        cur = self.conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS source_registry("
            "source_id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "source_name TEXT UNIQUE NOT NULL, source_type TEXT NOT NULL)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS categories("
            "category_id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "category_name TEXT UNIQUE NOT NULL)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS languages("
            "language_id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "language_name TEXT UNIQUE NOT NULL)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS library_authors("
            "author_id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS library_books("
            "book_id INTEGER PRIMARY KEY, title TEXT NOT NULL, author_id INTEGER NOT NULL,"
            "category_id INTEGER, publication_year INTEGER,"
            "source_id INTEGER NOT NULL, collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "FOREIGN KEY(author_id) REFERENCES library_authors(author_id),"
            "FOREIGN KEY(category_id) REFERENCES categories(category_id),"
            "FOREIGN KEY(source_id) REFERENCES source_registry(source_id))"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS github_repositories("
            "repo_full_name TEXT PRIMARY KEY,"
            "stars INTEGER NOT NULL, forks INTEGER NOT NULL,language_id INTEGER,"
            "source_id INTEGER NOT NULL,"
            "FOREIGN KEY(language_id) REFERENCES languages(language_id),"
            "FOREIGN KEY(source_id) REFERENCES source_registry(source_id))"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS web_books("
            "web_book_id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL,"
            "price REAL NOT NULL, rating INTEGER NOT NULL,"
            "category_id INTEGER, source_id INTEGER NOT NULL,"
            "FOREIGN KEY(category_id) REFERENCES categories(category_id),"
            "FOREIGN KEY(source_id) REFERENCES source_registry(source_id))"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS collection_logs("
            "log_id INTEGER PRIMARY KEY AUTOINCREMENT, source_id INTEGER, operation TEXT NOT NULL,"
            "status TEXT NOT NULL, records_collected INTEGER DEFAULT 0, error_message TEXT,"
            "started_at TEXT, finished_at TEXT, duration_sec REAL,"
            "FOREIGN KEY(source_id) REFERENCES source_registry(source_id))"
        )
        self.conn.commit()

    def _seed_sources(self):
        cur = self.conn.cursor()
        for name, typ in [
            ("library.db", "database"),
            ("GitHub API", "api"),
            ("Books to Scrape", "web"),
        ]:
            cur.execute(
                "INSERT OR IGNORE INTO source_registry(source_name,source_type) VALUES(?,?)",
                (name, typ),
            )
        self.conn.commit()
        rows = cur.execute("SELECT source_id,source_name FROM source_registry").fetchall()
        return {r["source_name"]: r["source_id"] for r in rows}
    


    def _log(self, source_name, operation, status, records=0, err=None, start=None):
        end = datetime.now()
        dur = (end - start).total_seconds() if start else None
        self.conn.execute(
            "INSERT INTO collection_logs(source_id,operation,status,records_collected,error_message,"
            "started_at,finished_at,duration_sec) VALUES(?,?,?,?,?,?,?,?)",
            (
                self.source_ids.get(source_name),
                operation,
                status,
                records,
                err,
                start.isoformat() if start else None,
                end.isoformat(),
                dur,
            ),
        )
        self.conn.commit()

    def _get_or_create_category(self, name):
        name = name or "Unknown"
        self.conn.execute(
            "INSERT OR IGNORE INTO categories(category_name) VALUES(?)", (name,)
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT category_id FROM categories WHERE category_name=?", (name,)
        ).fetchone()
        return int(row["category_id"])

    def _get_or_create_language(self, name):
        if not name:
            return None
        name = name.strip()
        self.conn.execute(
            "INSERT OR IGNORE INTO languages(language_name) VALUES(?)", (name,)
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT language_id FROM languages WHERE language_name=?", (name,)
        ).fetchone()
        return int(row["language_id"]) if row else None


    def collect_from_database(self, source_db_path=None):
        start = datetime.now()
        src = source_db_path or "notebooks/library.db"

        try:
            self.logger.info("starting database collection from %s", src)
            conn = sqlite3.connect(src)
            authors = pd.read_sql_query(
                "select author_id,name from authors", conn
            )
            books = pd.read_sql_query(
                "select b.book_id,b.title,b.author_id,b.publication_year,b.genre FROM books b",
                conn,
            )
            # print(books['copies_available'].head())
            conn.close()
            self.conn.executemany(
                "INSERT OR REPLACE INTO library_authors(author_id,name)"
                " VALUES(?,?)",
                authors.itertuples(index=False, name=None),
            )
            rows = []
            sid = self.source_ids["library.db"]
            for r in books.to_dict("records"):
                rows.append(
                    (
                        int(r["book_id"]),
                        r["title"],
                        int(r["author_id"]),
                        self._get_or_create_category(r.get("genre")),
                        int(r["publication_year"]) if pd.notna(r["publication_year"]) else None,
                        sid,
                    )
                )
            self.conn.executemany(
                "INSERT OR REPLACE INTO library_books(book_id,title,author_id,category_id,"
                "publication_year,source_id)"
                " VALUES(?,?,?,?,?,?)",
                rows,
            )
            self.conn.commit()
            self.logger.info("database collection completed: %s books", len(rows))
            self._log("library.db", "collect_from_database", "success", len(rows), start=start)
            return books
        
        except Exception as exc:
            self.logger.exception("DB collection failed: %s", exc)
            self._log("library.db", "collect_from_database", "error", err=str(exc), start=start)
            return pd.DataFrame()
        


    def _request_json(self, url, params=None, attempts=3):
        for i in range(attempts):
            try:
                r = self.session.get(url, params=params, timeout=25)
                if r.status_code == 429:
                    wait = 2**i
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                return r.json()
            except Exception:
                time.sleep(2**i)
        return None

    def collect_from_api(self, queries=None, per_query=25):
        start = datetime.now()
        queries = queries or [
            "book recommendation",
            "library management",
            "ebook reader",
            "book data pipeline",
        ]
        try:
            self.logger.info("starting git api collection")
            repos = {}
            for q in queries:
                data = self._request_json(
                    "https://api.github.com/search/repositories",
                    params={
                        "q": f"{q} in:name,description,readme",
                        "sort": "stars",
                        "order": "desc",
                        "per_page": per_query,
                    },
                )
                for it in (data or {}).get("items", []):
                    full = it.get("full_name")
                    if not full:
                        continue
                    repos[full] = it
                time.sleep(1)
            sid = self.source_ids["GitHub API"]
            rows = []
            for it in repos.values():
                rows.append(
                    (
                        it.get("full_name"),
                        int(it.get("stargazers_count", 0)),
                        int(it.get("forks_count", 0)),
                        self._get_or_create_language(it.get("language")),
                        sid,
                    )
                )
            self.conn.executemany(
                "INSERT OR REPLACE INTO github_repositories(repo_full_name,stars,forks,language_id,source_id)"
                " VALUES(?,?,?,?,?)",
                rows,
            )
            self.conn.commit()
            self.logger.info("API collection completed: %s repositories", len(rows))
            self._log("GitHub API", "collect_from_api", "success", len(rows), start=start)
            return pd.DataFrame(rows, columns=[
                "repo_full_name", "stars", "forks","language_id","source_id"
            ])
        except Exception as exc:
            self.logger.exception("API collection failed: %s", exc)
            self._log("GitHub API", "collect_from_api", "error", err=str(exc), start=start)
            return pd.DataFrame()
        



    def _can_scrape(self, url):
        try:
            p = urlparse(url)
            rp = RobotFileParser()
            rp.set_url(f"{p.scheme}://{p.netloc}/robots.txt")
            rp.read()
            return rp.can_fetch("*", url)
        except Exception:
            return False

    def _request_html(self, url, attempts=3):
        for i in range(attempts):
            try:
                r = self.session.get(url, timeout=25)
                r.raise_for_status()
                return BeautifulSoup(r.text, "lxml")
            except Exception:
                time.sleep(2**i)
        return None

    def collect_from_web(self, categories=None, max_pages_per_category=2):
        start = datetime.now()
        categories = categories or [
            "Travel","Mystery","Historical Fiction","Science Fiction",
            "Fiction","Classics","Romance","Fantasy"
        ]
        try:
            self.logger.info("Starting web collection for %s categories", len(categories))
            home = "http://books.toscrape.com/index.html"
            home_soup = self._request_html(home)
            cat_map = {}
            for a in home_soup.select("div.side_categories ul li ul li a"):
                name = " ".join(a.get_text(strip=True).split())
                cat_map[name.lower()] = (name, urljoin(home, a.get("href")))
            rows = []
            sid = self.source_ids["Books to Scrape"]
            for c in categories:
                if c.lower() not in cat_map:
                    continue
                cname, url = cat_map[c.lower()]
                if not self._can_scrape(url):
                    continue
                cid = self._get_or_create_category(cname)
                page = 1
                while url and page <= max_pages_per_category:
                    soup = self._request_html(url)
                    if soup is None:
                        break
                    for art in soup.select("article.product_pod"):
                        a = art.select_one("h3 a")
                        price_txt = (art.select_one(".price_color").get_text(strip=True) if art.select_one(".price_color") else "")
                        rating_el = art.select_one(".star-rating")
                        title = a.get("title", "").strip() if a else ""
                        price = float(re.sub(r"[^0-9.]", "", price_txt) or 0)
                        cls = rating_el.get("class", []) if rating_el else []
                        rating = {"One":1,"Two":2,"Three":3,"Four":4,"Five":5}.get(cls[1] if len(cls)>1 else "", 0)
                        purl = urljoin(url, a.get("href")) if a else ""
                        if not title or price <= 0 or rating < 1 or rating > 5 or not purl :
                            continue
                        rows.append((title, price, rating, cid, sid))
                    nxt = soup.select_one("li.next a")
                    url = urljoin(url, nxt["href"]) if nxt else None
                    page += 1
                    if url:
                        time.sleep(1)
            self.conn.executemany(
                "INSERT OR IGNORE INTO web_books(title,price,rating,category_id,source_id)"
                " VALUES(?,?,?,?,?)",
                rows,
            )
            self.conn.commit()
            self.logger.info("Web collection completed: %s books", len(rows))
            self._log("Books to Scrape", "collect_from_web", "success", len(rows), start=start)
            return pd.DataFrame(rows, columns=["title","price","rating","category_id","source_id"])
        except Exception as exc:
            self.logger.exception("Web collection failed: %s", exc)
            self._log("Books to Scrape", "collect_from_web", "error", err=str(exc), start=start)
            return pd.DataFrame()



    def get_collection_stats(self):
        c = self.conn.cursor()
        return {
            "library_books": c.execute("SELECT COUNT(*) FROM library_books").fetchone()[0],
            "github_repositories": c.execute("SELECT COUNT(*) FROM github_repositories").fetchone()[0],
            "web_books": c.execute("SELECT COUNT(*) FROM web_books").fetchone()[0],
            "logs": pd.read_sql_query(
                "SELECT sr.source_name,cl.operation,cl.status,cl.records_collected,cl.duration_sec "
                "FROM collection_logs cl LEFT JOIN source_registry sr ON cl.source_id=sr.source_id "
                "ORDER BY cl.log_id",
                self.conn,
            ),
        }

    def export_tables(self, output_dir="exports"):
        os.makedirs(output_dir, exist_ok=True)
        tables = [
            "source_registry","categories","languages","library_authors",
            "library_books","github_repositories","web_books","collection_logs"
        ]
        for t in tables:
            pd.read_sql_query(f"SELECT * FROM {t}", self.conn).to_csv(
                os.path.join(output_dir, f"{t}.csv"), index=False
            )
        self.logger.info("Exported all tables to %s", output_dir)
        
        

    def generate_analysis_report(self, output_html="analysis.html", assets_dir="analysis_assets"):
        self.logger.info("generating analysis report at %s", output_html)
        os.makedirs(assets_dir, exist_ok=True)


        # get category and books info from database
        lib = pd.read_sql_query(
            "SELECT lb.book_id,lb.publication_year,c.category_name genre FROM library_books lb "
            "LEFT JOIN categories c ON lb.category_id=c.category_id", self.conn
        )

        # get books info from web
        web = pd.read_sql_query(
            "SELECT wb.title,wb.price,wb.rating,dc.category_name category FROM web_books wb "
            "LEFT JOIN categories dc ON wb.category_id=dc.category_id", self.conn
        )

        # get github repos info from api
        api = pd.read_sql_query(
            "SELECT github.repo_full_name,github.stars,github.forks,l.language_name language FROM github_repositories github "
            "LEFT JOIN languages l ON github.language_id=l.language_id", self.conn
        )
        figs = []

        # popular geners 
        f1 = os.path.join(assets_dir, "popular_genres.png")
        pd.concat([lib["genre"].dropna(), web["category"].dropna()]).value_counts().head(12).plot(kind="bar", figsize=(10,5), title="Popular Genres")
        plt.tight_layout(); plt.savefig(f1, dpi=180); plt.close(); figs.append(f1)


        # price by category
        f2 = os.path.join(assets_dir, "price_by_category.png")
        web.groupby("category")["price"].mean().sort_values(ascending=False).plot(kind="bar", figsize=(10,5), title="Average Web Price by Category")
        plt.tight_layout(); plt.savefig(f2, dpi=180); plt.close(); figs.append(f2)


        # rating distribution
        f3 = os.path.join(assets_dir, "rating_distribution.png")
        web["rating"].value_counts().sort_index().plot(kind="bar", figsize=(8,5), title="Web Books Rating Distribution")
        plt.tight_layout(); plt.savefig(f3, dpi=180); plt.close(); figs.append(f3)


        # top github languages
        f4 = os.path.join(assets_dir, "github_languages.png")
        api["language"].dropna().value_counts().head(10).plot(kind="bar", figsize=(10,5), title="Top GitHub Languages")
        plt.tight_layout(); plt.savefig(f4, dpi=180); plt.close(); figs.append(f4)

        
        f5 = os.path.join(assets_dir, "stars_vs_forks.png")
        plt.figure(figsize=(8,6)); plt.scatter(api["stars"], api["forks"], alpha=0.6); plt.title("GitHub Stars vs Forks"); plt.xlabel("Stars"); plt.ylabel("Forks")
        plt.tight_layout(); plt.savefig(f5, dpi=180); plt.close(); figs.append(f5)

        f6 = os.path.join(assets_dir, "publication_timeline.png")
        lib.dropna(subset=["publication_year"]).groupby("publication_year")["book_id"].count().sort_index().plot(kind="line", marker="o", figsize=(10,5), title="Library Publication Timeline")
        plt.tight_layout(); plt.savefig(f6, dpi=180); plt.close(); figs.append(f6)

        stats = self.get_collection_stats()
        top_genre = (pd.concat([lib["genre"].dropna(), web["category"].dropna()]).value_counts().index[0])
        top_lang = api["language"].dropna().value_counts().index[0] if not api.empty else "Unknown"
        avg_price = web["price"].mean() if not web.empty else 0
        five_star = (web["rating"] == 5).mean() * 100 if not web.empty else 0
        top_repo = api.sort_values("stars", ascending=False).iloc[0]["repo_full_name"] if not api.empty else "N/A"
        logs_html = stats["logs"].to_html(index=False)
        imgs = "\n".join([f'<h3>Visualization {i+1}</h3><img src="{p}" style="max-width:100%;">' for i, p in enumerate(figs)])
        html = (
            "<html>"
                "<head>"
                    "<meta charset='utf-8'>"
                    "<title>Book Market Intelligence</title>"
                "</head>"
                "<body>"
                    "<h1>Book Market Intelligence Report</h1>"
                    f"<p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>"
                    "<h2>Executive Summary</h2>"
                    f"<p>Integrated 3 sources into market_intelligence.db. Collected {stats['library_books']} library books, {stats['github_repositories']} GitHub repos, and {stats['web_books']} web books.</p>"
                    "<h2>Collection Statistics</h2>"
                    f"<ul><li>Library books: {stats['library_books']}</li><li>GitHub repos: {stats['github_repositories']}</li><li>Web books: {stats['web_books']}</li></ul>"
                    f"{logs_html}"
                    "<h2>Market Insights</h2>"
                    f"<ul><li>Popular genres: {top_genre}</li><li>Average web price: {avg_price:.2f}</li><li>Five-star share: {five_star:.2f}%</li><li>Top GitHub language: {top_lang}</li><li>Top repo: {top_repo}</li></ul>"
                    "<h2>Visualizations</h2>"
                    f"{imgs}"
                    "</body></html>"
        )
        with open(output_html, "w", encoding="utf-8") as f:
            f.write(html)
        self.logger.info("Analysis report generated with %s visualizations", len(figs))
        return output_html

    def run(self, library_db_path=None, web_categories=None):
        self.logger.info("Running integrated pipeline")
        d1 = self.collect_from_database(library_db_path)
        d2 = self.collect_from_api()
        d3 = self.collect_from_web(web_categories)
        self.export_tables("exports")
        report = self.generate_analysis_report("analysis.html", "analysis_assets")
        self.logger.info("Integrated pipeline finished")
        return {"library": len(d1), "api": len(d2), "web": len(d3), "report": report}

    def close(self):
        self.conn.close()
        self.session.close()
        self.logger.info("Pipeline closed")


def run_pipeline():
    p = DataCollectionPipeline(db_path="market_intelligence.db")
    try:
        result = p.run(library_db_path=os.path.join("notebooks", "library.db"))
        print("Pipeline finished.", result)
        return result
    finally:
        p.close()


if __name__ == "__main__":
    run_pipeline()
