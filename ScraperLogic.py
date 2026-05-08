import requests
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import urlparse,urljoin
from collections import deque
import json

class WebScraperCore:
    def __init__(self,start_url, max_pages,delay,selected_tags,structured_var,job_id, update_callback):
        self.start_url = start_url
        self.max_pages = max_pages
        self.delay = delay
        self.selected_tags = selected_tags
        self.structured_var = structured_var
        self.job_id = job_id
        self.update_callback = update_callback

        self.scraped_data= {}
        self.is_scraping = True
        self.visited_urls = set()
        self.queue = deque()


    def _clean_text(self, value):
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()

    def _extract_jsonld_nodes(self, soup):
        nodes = []
        for script in soup.find_all("script", type="application/ld+json"):
            raw_json = script.string or script.get_text(strip=True)
            if not raw_json:
                continue
            try:
                parsed = json.loads(raw_json)
            except json.JSONDecodeError:
                continue
            stack = parsed if isinstance(parsed, list) else [parsed]
            while stack:
                node = stack.pop()
                if isinstance(node, dict):
                    if "@graph" in node and isinstance(node["@graph"], list):
                        stack.extend(node["@graph"])
                    nodes.append(node)
                    for value in node.values():
                        if isinstance(value, (dict, list)):
                            stack.append(value)
                elif isinstance(node, list):
                    stack.extend(node)
        return nodes

    def _extract_offer_data(self, offer_node):
        if not isinstance(offer_node, dict):
            return {}
        price = self._clean_text(offer_node.get("price"))
        currency = self._clean_text(offer_node.get("priceCurrency"))
        availability = self._clean_text(offer_node.get("availability"))
        if availability:
            availability = availability.split("/")[-1]
        return {
            "price": price,
            "currency": currency,
            "availability": availability
        }

    def _extract_best_product_jsonld(self, soup):
        best = None
        for node in self._extract_jsonld_nodes(soup):
            node_type = node.get("@type")
            is_product = "Product" in node_type if isinstance(node_type, list) else node_type == "Product"
            if not is_product:
                continue

            offers = node.get("offers")
            if isinstance(offers, list):
                offer_data = self._extract_offer_data(offers[0]) if offers else {}
            else:
                offer_data = self._extract_offer_data(offers)

            image_value = node.get("image")
            image_url = ""
            if isinstance(image_value, list) and image_value:
                first = image_value[0]
                image_url = self._clean_text(first.get("url") if isinstance(first, dict) else first)
            elif isinstance(image_value, dict):
                image_url = self._clean_text(image_value.get("url"))
            else:
                image_url = self._clean_text(image_value)

            brand = node.get("brand")
            if isinstance(brand, dict):
                brand = brand.get("name")

            additional = {}
            for item in node.get("additionalProperty", []) or []:
                if isinstance(item, dict):
                    key = self._clean_text(item.get("name"))
                    value = self._clean_text(item.get("value"))
                    if key and value:
                        additional[key] = value

            candidate = {
                "name": self._clean_text(node.get("name")),
                "category": self._clean_text(node.get("category")),
                "description": self._clean_text(node.get("description")),
                "sku": self._clean_text(node.get("sku")),
                "brand": self._clean_text(brand),
                "image": image_url,
                "price": offer_data.get("price", ""),
                "currency": offer_data.get("currency", ""),
                "availability": offer_data.get("availability", ""),
                "additional_properties": additional
            }
            if not best or (len(candidate["description"]) > len(best.get("description", ""))):
                best = candidate
        return best or {}

    def _extract_products_from_jsonld(self, soup):
        products = []
        best = self._extract_best_product_jsonld(soup)
        if best.get("name"):
            products.append({
                "name": best.get("name", ""),
                "category": best.get("category", ""),
                "description": best.get("description", ""),
                "price": best.get("price", ""),
                "currency": best.get("currency", ""),
                "availability": best.get("availability", ""),
                "sku": best.get("sku", ""),
                "brand": best.get("brand", ""),
                "image": best.get("image", ""),
                "additional_properties": best.get("additional_properties", {})
            })
        return products

    def _extract_products_from_dom(self, soup):
        products = []
        selectors = [
            '[itemtype*="Product"]',
            ".product",
            ".product-item",
            ".product-card",
            "[data-product-name]"
        ]
        for selector in selectors:
            for block in soup.select(selector):
                name = (
                    block.get("data-product-name")
                    or (block.select_one('[itemprop="name"]') or block.select_one(".product-name") or block.find(["h1", "h2", "h3"]))
                )
                category = block.select_one('[itemprop="category"]') or block.select_one(".category")
                description = block.select_one('[itemprop="description"]') or block.select_one(".description") or block.find("p")

                if hasattr(name, "get_text"):
                    name = name.get_text(" ", strip=True)
                if hasattr(category, "get_text"):
                    category = category.get_text(" ", strip=True)
                if hasattr(description, "get_text"):
                    description = description.get_text(" ", strip=True)

                product = {
                    "name": self._clean_text(name),
                    "category": self._clean_text(category),
                    "description": self._clean_text(description)
                }
                if product["name"] and (product["category"] or product["description"]):
                    products.append(product)
        return products

    def _deduplicate_products(self, products):
        deduped = []
        seen = set()
        for product in products:
            key = (
                product.get("name", "").lower(),
                product.get("category", "").lower(),
                product.get("description", "").lower(),
                product.get("product_url", "").lower()
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(product)
        return deduped

    def _looks_like_product_url(self, full_url):
        parsed = urlparse(full_url)
        if parsed.scheme not in ("http", "https"):
            return False
        if "add-to-cart=" in parsed.query:
            return False
        path = parsed.path.strip("/")
        if not path:
            return False
        parts = [segment for segment in path.split("/") if segment]
        if not parts:
            return False

        blocklist = {
            "shop", "product-category", "category", "tag", "brands", "brand",
            "cart", "checkout", "my-account", "account", "wishlist", "compare",
            "contact", "about", "services", "blog", "feed", "wp-admin", "wp-json",
            "privacy-policy", "refund_returns", "track-order", "kilimohow"
        }
        if any(part.lower() in blocklist for part in parts):
            return False

        slug = parts[-1].lower()
        if slug in blocklist:
            return False

        return bool(re.search(r"[a-z0-9]+(?:-[a-z0-9]+)+", slug) or re.search(r"\d", slug))

    def _extract_product_links_from_listing(self, soup, current_url):
        product_links = set()
        selectors = [
            "a.woocommerce-LoopProduct-link",
            "h2.woocommerce-loop-product__title a",
            "li.product a[href]",
            ".related.products a.woocommerce-LoopProduct-link"
        ]
        for selector in selectors:
            for tag in soup.select(selector):
                href = tag.get("href")
                if not href:
                    continue
                full_url = urljoin(current_url, href)
                if not self._looks_like_product_url(full_url):
                    continue
                product_links.add(full_url)
        return product_links

    def _extract_listing_pagination_links(self, soup, current_url):
        listing_links = set()
        selectors = [
            "a.next.page-numbers",
            ".woocommerce-pagination a.page-numbers",
            "a[rel='next']"
        ]
        for selector in selectors:
            for tag in soup.select(selector):
                href = tag.get("href")
                if not href:
                    continue
                listing_links.add(urljoin(current_url, href))
        return listing_links

    def _extract_product_from_detail_page(self, soup, current_url):
        jsonld_product = self._extract_best_product_jsonld(soup)
        name_tag = soup.select_one("h1.product_title") or soup.select_one("h1.entry-title")
        name = self._clean_text(name_tag.get_text(" ", strip=True) if name_tag else "") or jsonld_product.get("name", "")
        if not name:
            return None

        category_tags = soup.select(".posted_in a, .meta-cat a")
        if not category_tags:
            category_tags = soup.select("nav.woocommerce-breadcrumb a")
        categories = [self._clean_text(tag.get_text(" ", strip=True)) for tag in category_tags if self._clean_text(tag.get_text(" ", strip=True))]
        if categories and categories[0].lower() == "home":
            categories = categories[1:]
        if categories:
            categories = categories[:-1] if len(categories) > 1 else categories

        short_desc = soup.select_one(".woocommerce-product-details__short-description")
        long_desc = soup.select_one("#tab-description")
        description_meta = soup.find("meta", attrs={"name": "description"})
        description = ""
        if short_desc:
            description = self._clean_text(short_desc.get_text(" ", strip=True))
        elif long_desc:
            description = self._clean_text(long_desc.get_text(" ", strip=True))
        elif description_meta:
            description = self._clean_text(description_meta.get("content", ""))
        if not description:
            description = jsonld_product.get("description", "")

        price_tag = soup.select_one(".summary p.price .woocommerce-Price-amount, p.price .woocommerce-Price-amount") or soup.select_one("p.price")
        price_meta = soup.find("meta", attrs={"property": "product:price:amount"})
        stock_tag = soup.select_one(".stock")
        stock_meta = soup.find("meta", attrs={"property": "product:availability"})
        sku_tag = soup.select_one(".sku") or soup.select_one("[itemprop='sku']")
        image_meta = soup.find("meta", attrs={"property": "og:image"})
        currency_meta = soup.find("meta", attrs={"property": "product:price:currency"})
        brand_tag = soup.select_one(".product_meta .brand a, .product_meta .brand")

        return {
            "name": name,
            "category": ", ".join(categories) if categories else jsonld_product.get("category", ""),
            "categories": categories,
            "description": description,
            "price": self._clean_text(price_tag.get_text(" ", strip=True) if price_tag else "") or self._clean_text(price_meta.get("content", "") if price_meta else "") or jsonld_product.get("price", ""),
            "currency": self._clean_text(currency_meta.get("content", "") if currency_meta else "") or jsonld_product.get("currency", ""),
            "availability": self._clean_text(stock_tag.get_text(" ", strip=True) if stock_tag else "") or self._clean_text(stock_meta.get("content", "") if stock_meta else "") or jsonld_product.get("availability", ""),
            "sku": self._clean_text(sku_tag.get_text(" ", strip=True) if sku_tag else "") or jsonld_product.get("sku", ""),
            "brand": self._clean_text(brand_tag.get_text(" ", strip=True) if brand_tag else "") or jsonld_product.get("brand", ""),
            "image": self._clean_text(image_meta.get("content", "") if image_meta else "") or jsonld_product.get("image", ""),
            "additional_properties": jsonld_product.get("additional_properties", {}),
            "product_url": current_url,
            "source_url": current_url
        }

    def stop_scraping(self):
        self.is_scraping = False

    def _run_crawl(self, products_only=False):
        # initial setup
        self.queue.append(self.start_url)
        self.visited_urls.add(self.start_url)
        domain = urlparse(self.start_url).netloc
        pages_scraped = 0
        max_retries = 3

        #initial status update
        self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data)

        while self.queue and pages_scraped < self.max_pages and self.is_scraping:
            current_url = self.queue.popleft()
            self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data,
                                 log_message=f"Attempting to scrape: {current_url}")

            soup=None
            for attempt in range(max_retries):
                if not self.is_scraping: break
                try:
                    #1. Fetch Page (Single Request)
                    response = requests.get(current_url, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'lxml')
                    break # Successful fetch
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data,
                                             log_message=f"Failed to fetch {current_url}. Retrying... {attempt + 1}: {e}")
                        time.sleep(2)
                        continue
                    self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data,
                                         log_message=f"Failed to fetch {current_url} ")
                except Exception as e:
                    self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data,
                                         log_message=f"Error Processing {current_url}: {e} ")
                    break
            if soup and self.is_scraping:
                products = self._deduplicate_products(
                    self._extract_products_from_jsonld(soup) + self._extract_products_from_dom(soup)
                )
                if products_only:
                    detail_product = self._extract_product_from_detail_page(soup, current_url)
                    if detail_product:
                        products = self._deduplicate_products(products + [detail_product])

                    self.scraped_data[current_url] = {
                        'products': products
                    }
                else:
                    # 2. Scrape Content (Text, Table,Metadata)
                    content=[]
                    for tag in self.selected_tags:
                        elements =soup.find_all(tag)
                        for element in elements:
                            text = element.get_text(strip=True)
                            if text:
                                content.append(text)

                    cleaned_content=[re.sub(r'\s+',' ',text) for text in content]
                    text_content = '\n'.join(list(dict.fromkeys(cleaned_content)))

                    tables=[]
                    metadata=[]
                    if self.structured_var:
                        for table in soup.find_all('table'):
                            rows=[]
                            for row in table.find_all('tr'):
                                cells =[td.get_text(strip=True) for td in row.find_all('td','th')]
                                if cells:
                                    rows.append(cells)
                                if rows:tables.append(rows)

                        for meta in soup.find_all('meta'):
                            meta_data=[]
                            #meta extraction
                            if meta.get('name'): meta_data.append(meta.get('name'))
                            if meta.get('property'): meta_data.append(meta.get('property'))
                            if meta.get('content'): meta_data.append(meta.get('content'))
                            if meta_data:metadata.append(meta_data)

                    self.scraped_data[current_url] = {
                        'text' : text_content,
                        'tables' : tables,
                        'metadata' : metadata,
                        'products': products
                    }
                pages_scraped += 1
                # 3. Update status and find links
                self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data,
                                     log_message=f"Successfully scraped: {pages_scraped} pages"
                                     )
                #find links to crawl
                if products_only:
                    next_urls = set()
                    next_urls.update(self._extract_product_links_from_listing(soup, current_url))
                    next_urls.update(self._extract_listing_pagination_links(soup, current_url))
                    for full_url in next_urls:
                        if not self.is_scraping:
                            break
                        parsed_url = urlparse(full_url)
                        if parsed_url.netloc == domain and full_url not in self.visited_urls:
                            self.queue.append(full_url)
                            self.visited_urls.add(full_url)
                else:
                    for link in soup.find_all('a', href=True):
                        if not self.is_scraping: break
                        href = link['href']
                        full_url = urljoin(current_url, href)
                        parsed_url = urlparse(full_url)
                        if parsed_url.netloc == domain and full_url not in self.visited_urls:
                            self.queue.append(full_url)
                            self.visited_urls.add(full_url)

            if self.delay >0 and self.is_scraping:
                time.sleep(self.delay)
        # Final status update
        final_status ='STOPPED' if not self.is_scraping else 'FINISHED'
        self.update_callback(self.job_id,final_status,pages_scraped,self.scraped_data,
                             log_message=f"Scraping finished. Status : {final_status}")

    def crawl_and_scrape(self):
        self._run_crawl(products_only=False)

    def crawl_products_only(self):
        self._run_crawl(products_only=True)
