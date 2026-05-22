def paginate(page: int, page_size: int, total: int) -> dict:
    page = max(1, page)
    page_size = min(max(1, page_size), 100)
    pages = max(1, (total + page_size - 1) // page_size)
    return {"page": page, "page_size": page_size, "pages": pages, "total": total}
