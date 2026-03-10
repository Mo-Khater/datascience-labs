import time
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import matplotlib.pyplot as plt
import os
import json
import logging
from datetime import datetime
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser


def parse_rating(article):
    word_to_number = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
    rating_element = article.select_one('.star-rating')
    if not rating_element:
        return 0
    classes = rating_element.get('class', [])
    rating_word = classes[1] if len(classes) > 1 else None
    # print(rating_word)
    return word_to_number.get(rating_word, 0)


def parse_price(article):
    price_text = article.select_one('.price_color').get_text(strip=True)
    # print(f"{price_text}")
    clean_text = re.sub(r'[^0-9.]', '', price_text)
    return float(clean_text)


def scrape_travel_books():
    """
    Scrape all travel books.

    Requirements:
    - Handle pagination
    - Add 1 second delay between pages
    - Extract all required fields

    Returns:
        DataFrame with book data
    """
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Educational) Part3Scraper/1.0'})

    books = []
    page_url = 'http://books.toscrape.com/catalogue/category/books/travel_2/index.html'

    while page_url:
        response = session.get(page_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        for article in soup.select('article.product_pod'):
            title = article.select_one('h3 a')
            availability = article.select_one('.availability').get_text(' ', strip=True)


            books.append(
                {
                    'title': title.get('title', '').strip(),
                    'price': parse_price(article),
                    'rating': parse_rating(article),
                    'availability': availability,
                }
            )

        next_link = soup.select_one('li.next a')


        page_url = urljoin(page_url, next_link['href']) if next_link else None


        if page_url:
            time.sleep(1)

    return pd.DataFrame(books)


df_travel = scrape_travel_books()
df_travel.to_csv('task1_travel_books.csv', index=False)



print(df_travel.head())

avg_price = df_travel['price'].mean()
five_star_books = (df_travel['rating'] == 5).sum()
in_stock_percentage = df_travel['availability'].str.contains('in stock',case=False, na=False).mean()


analysis_text = (
    f"average price {avg_price:.2f}\n"
    f"number of 5-star books: {five_star_books}\n"
    f"in stock percentage : {in_stock_percentage:.2f}%\n"
)

with open('task1_analysis.txt', 'w', encoding='utf-8') as f:
    f.write(analysis_text)


# task 2


class CategoryScraper:
    """
    Scrape multiple categories and compare.
    """

    def __init__(self):
        self.base_url = 'http://books.toscrape.com'
        self.session = requests.Session()

        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Educational) CategoryScraper/1.0'})
        self.word_to_number = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
        self.category_urls = self._get_category_urls()

    def _get_category_urls(self):
        url = f'{self.base_url}/index.html'
        response = self.session.get(url, timeout=10)

        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        mapping = {}
        for link in soup.select('div.side_categories a'):
            name = link.get_text(strip=True)
            if not name:
                continue
            mapping[name.lower()] = urljoin(url, link.get('href'))
        return mapping

    def _parse_price(self, article):
        text = article.select_one('.price_color').get_text(strip=True)
        clean = re.sub(r'[^0-9.]', '', text)
        return float(clean)

    def _parse_rating(self, article):
        classes = article.select_one('.star-rating').get('class', [])
        word = classes[1] if len(classes) > 1 else None
        return self.word_to_number.get(word, 0)


    def scrape_category(self, category_name, max_pages=2):
        page_url = self.category_urls.get(category_name.lower())

        if not page_url:
            raise ValueError(f'not found: {category_name}')

        books = []
        page_num = 1

        while page_url and page_num <= max_pages:

            response = self.session.get(page_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')

            for article in soup.select('article.product_pod'):
                title = article.select_one('h3 a').get('title', '').strip()
                books.append(
                    {
                        'title': title,
                        'price': self._parse_price(article),
                        'rating': self._parse_rating(article),
                        'category': category_name,
                    }
                )

            next_link = soup.select_one('li.next a')
            page_url = urljoin(page_url, next_link['href']) if next_link else None
            page_num += 1
            if page_url and page_num <= max_pages:
                time.sleep(1)

        return books

    def scrape_multiple_categories(self, categories):

        all_books = []
        for category in categories:
            all_books.extend(self.scrape_category(category, max_pages=2))
            time.sleep(1)
        return pd.DataFrame(all_books)

    def compare_categories(self, df):

        stats_df = (
            df.groupby('category', as_index=False)
            .agg(
                avg_price=('price', 'mean'),
                avg_rating=('rating', 'mean'),
            )
        )


        highest_rating_category = stats_df.loc[stats_df['avg_rating'].idxmax(), 'category']
        highest_price_category = stats_df.loc[stats_df['avg_price'].idxmax(), 'category']

        fig, axes = plt.subplots(1, 2, figsize=(16, 5))
        axes[0].bar(stats_df['category'], stats_df['avg_rating'])
        axes[0].set_title('Average Rating')
        axes[0].tick_params(axis='x', rotation=27)

        axes[1].bar(stats_df['category'], stats_df['avg_price'])
        axes[1].set_title('Average Price')
        axes[1].tick_params(axis='x', rotation=27)

        fig.tight_layout()
        fig.savefig('task2_comparison.png', dpi=200, bbox_inches='tight')
        plt.close(fig)

        return {
            'highest_average_rating': highest_rating_category,
            'highest_average_price': highest_price_category,
            'stats_table': stats_df,
        }


# Task 2 outputs
categories = ['Fiction', 'Mystery', 'Historical Fiction', 'Science Fiction']
category_scraper = CategoryScraper()
df_categories = category_scraper.scrape_multiple_categories(categories)
df_categories.to_csv('task2_categories.csv', index=False)

comparison = category_scraper.compare_categories(df_categories)



# task3

class AdvancedBookScraper:

    def __init__(self, output_dir='.'):
        """
        Initialize scraper with logging and rate limiting.
        """
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.logger = logging.getLogger('AdvancedBookScraper')
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fh = logging.FileHandler('scraper.log', encoding='utf-8')
            fh.setFormatter(
                logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            )
            self.logger.addHandler(fh)

        self.base_url = 'http://books.toscrape.com'
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Educational) AdvancedBookScraper/1.0'})

        self.request_timestamps = []
        self.progress_tracker = {}
        self.rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}


    def _respect_rate_limit(self):
        now = time.time()
        self.request_timestamps = [t for t in self.request_timestamps if now - t < 60]
        if len(self.request_timestamps) >= 10:
            sleep_for = 60 - (now - self.request_timestamps[0])
            if sleep_for > 0:
                self.logger.info(f'Rate limit reached; sleeping {sleep_for:.2f}s')
                time.sleep(sleep_for)
            now = time.time()
            self.request_timestamps = [t for t in self.request_timestamps if now - t < 60]
        self.request_timestamps.append(time.time())



    def _category_url_map(self):
        soup = self.scrape_with_retry(f'{self.base_url}/index.html')
        if soup is None:
            return {}
        mapping = {}
        for link in soup.select('div.side_categories a'):
            name = link.get_text(strip=True)
            if not name or name.lower() == 'books':
                continue
            mapping[name.lower()] = urljoin(f'{self.base_url}/index.html', link.get('href'))
        return mapping

    def check_robots_txt(self, url):
        parsed = urlparse(url)
        robots_url = f'{parsed.scheme}://{parsed.netloc}/robots.txt'

        try:
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            allowed = rp.can_fetch('*', url)
            self.logger.info(f'robots.txt check for {url}: {allowed}')
            return allowed
        except Exception as e:
            self.logger.error(f'robots.txt check failed: {e}')
            return False


    def scrape_with_retry(self, url, max_attempts=3):
        """
        scape with backoff 1,2,4,...
        """

        for attempt in range(max_attempts):
            try:
                self._respect_rate_limit()
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return BeautifulSoup(response.text, 'lxml')
            except Exception as e:
                wait = 2**attempt
                self.logger.error(
                    f'attempt {attempt + 1}/{max_attempts} failed for {url}: {e}'
                )
                if attempt < max_attempts - 1:
                    time.sleep(wait)
        return None


    def _parse_price(self, article):
        text = article.select_one('.price_color').get_text(strip=True)
        clean = re.sub(r'[^0-9.]', '', text)
        return float(clean)

    def _parse_rating(self, article):
        classes = article.select_one('.star-rating').get('class', [])
        word = classes[1] if len(classes) > 1 else None
        return self.rating_map.get(word, 0)

    def validate_book_data(self, book):

        try:
            price_ok = isinstance(book.get('price'), (int, float)) and book['price'] > 0
            rating_ok = isinstance(book.get('rating'), int) and 1 <= book['rating'] <= 5
            title_ok = isinstance(book.get('title'), str) and len(book['title'].strip()) > 0
            return price_ok and rating_ok and title_ok
        except Exception:
            return False

    def save_progress(self, books, state=None, filename='progress.json'):

        payload = {
            'saved_at': datetime.now().isoformat(),
            'books': books,
            'state': state or {},
            'progress_tracker': self.progress_tracker,
        }
        path = os.path.join(self.output_dir, filename)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def load_progress(self, filename='progress.json'):

        path = os.path.join(self.output_dir, filename)
        if not os.path.exists(path):
            return {'books': [], 'state': {}, 'progress_tracker': {}}

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self.progress_tracker = data.get('progress_tracker', {})
        return data

    def export_data(self, books, base_filename='task3_books'):

        df = pd.DataFrame(books)

        csv_path = os.path.join(self.output_dir, f'{base_filename}.csv')
        xlsx_path = os.path.join(self.output_dir, f'{base_filename}.xlsx')
        json_path = os.path.join(self.output_dir, f'{base_filename}.json')

        df.to_csv(csv_path, index=False, encoding='utf-8')

        with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Books')

        payload = {
            'generated_at': datetime.now().isoformat(),
            'total_books': int(len(books)),
            'books': books,
        }
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(
                payload,
                f,
                ensure_ascii=False,
                indent=2,
            )

        return {'csv': csv_path, 'xlsx': xlsx_path, 'json': json_path}

    def run_full_pipeline(self, categories, max_pages_per_category=5):

        test_url = f'{self.base_url}/catalogue/page-1.html'
        if not self.check_robots_txt(test_url):
            raise RuntimeError('Scraping not allowed by robots.txt')

        progress = self.load_progress('progress.json')
        books = progress.get('books', [])
        state = progress.get('state', {})


        category_urls = self._category_url_map()
        if not category_urls:
            raise RuntimeError('cannot fetch category URLs')
        

        for category in categories:
            cat_key = category.lower()
            if cat_key not in category_urls:
                self.logger.error(f'skipping missing category: {category}')
                continue

            category_state = state.get(category, {})
            page_url = category_state.get('next_url') or category_urls[cat_key]
            page_num = int(category_state.get('next_page', 1))

            self.logger.info(f'start category {category} from page {page_num}')

            while page_url and page_num <= max_pages_per_category:
                page_id = f'{category}|{page_num}'
                soup = self.scrape_with_retry(page_url, max_attempts=3)
                if soup is None:
                    self.progress_tracker[page_id] = 'failed'
                    self.save_progress(books, state, 'progress.json')
                    self.logger.error(f'Failed page {page_id}; progress saved')
                    break



                for article in soup.select('article.product_pod'):
                    book = {
                        'title': article.select_one('h3 a').get('title', '').strip(),
                        'price': self._parse_price(article),
                        'rating': self._parse_rating(article),
                        'availability': article.select_one('.availability').get_text(' ', strip=True),
                        'category': category,
                    }



                    if self.validate_book_data(book):
                        books.append(book)
                    else:
                        self.logger.error(f'invalid data skipped: {book}')

                next_link = soup.select_one('li.next a')
                next_url = urljoin(page_url, next_link['href']) if next_link else None



                self.progress_tracker[page_id] = 'completed'
                state[category] = {
                    'next_page': page_num + 1,
                    'next_url': next_url,
                }
                self.save_progress(books, state, 'progress.json')
                self.logger.info(
                    f'completed {page_id}; added {len(books)} books; total {len(books)}'
                )

                page_url = next_url
                page_num += 1

        exports = self.export_data(books, base_filename='task3_books')

        summary_path = os.path.join(self.output_dir, 'task3_summary.txt')
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(f'total number of books: {len(books)}\n')
            f.write('books per category:\n')
            if books:
                counts = pd.DataFrame(books)['category'].value_counts()
                for cat, cnt in counts.items():
                    f.write(f'- {cat}: {cnt}\n')



        self.logger.info('pipeline completed successfully')
        return {
            'total_books': len(books),
            'exports': exports,
            'summary': summary_path,
            'progress_file': os.path.join(self.output_dir, 'progress.json'),
        }
    


scraper = AdvancedBookScraper()
result = scraper.run_full_pipeline(
    categories=['Mystery', 'Science Fiction', 'Fantasy'], max_pages_per_category=3
)
print(result)
