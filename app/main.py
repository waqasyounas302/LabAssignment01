from fastapi import FastAPI, HTTPException, Query, Request, Depends
from pydantic import BaseModel, Field
from pymongo import MongoClient
import time
from collections import Counter


client = MongoClient("mongodb://localhost:27017/")
db = client["goodbooks"]

app = FastAPI(title="GoodBooks API (MongoDB)")

class RatingIn(BaseModel):
    user_id: int
    book_id: int
    rating: int = Field(ge=1, le=5)

API_KEY = "mysecretkey"

def require_key(request: Request):
    if request.headers.get("x-api-key") != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    t0 = time.time()
    response = await call_next(request)
    dt = int((time.time() - t0) * 1000)
    print({"route": request.url.path, "status": response.status_code, "latency_ms": dt})
    return response

@app.get("/books")
def list_books(
    q: str | None = None,
    tag: str | None = None,
    min_avg: float | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    sort: str = Query("avg", pattern="^(avg|ratings_count|year|title)$"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    page: int = 1,
    page_size: int = Query(20, le=100)
):
    filt = {}
    if q:
        filt["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"authors": {"$regex": q, "$options": "i"}}
        ]
    if min_avg:
        filt["average_rating"] = {"$gte": float(min_avg)}
    year_clause = {}
    if year_from: year_clause["$gte"] = year_from
    if year_to: year_clause["$lte"] = year_to
    if year_clause: filt["original_publication_year"] = year_clause


    if tag:
        tag_doc = db.tags.find_one({"tag_name": {"$regex": tag, "$options": "i"}})
        if tag_doc:
            book_ids = [bt["goodreads_book_id"] for bt in db.book_tags.find({"tag_id": tag_doc["tag_id"]})]
            filt["goodreads_book_id"] = {"$in": book_ids}
        else:
            return {"items": [], "page": page, "page_size": page_size, "total": 0}

    sort_map = {
        "avg": "average_rating",
        "ratings_count": "ratings_count",
        "year": "original_publication_year",
        "title": "title"
    }
    direction = -1 if order == "desc" else 1

    total = db.books.count_documents(filt)
    items = list(db.books.find(filt).sort([(sort_map[sort], direction)]).skip((page - 1) * page_size).limit(page_size))
    for item in items:
        item["_id"] = str(item["_id"])

    return {"items": items, "page": page, "page_size": page_size, "total": total}

@app.get("/books/{book_id}")
def get_book(book_id: int):
    book = db.books.find_one({"book_id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    book["_id"] = str(book["_id"])
    return book

@app.get("/books/{book_id}/tags")
def get_book_tags(book_id: int):
    book = db.books.find_one({"book_id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    goodreads_id = book.get("goodreads_book_id")
    links = db.book_tags.find({"goodreads_book_id": goodreads_id})
    tag_ids = [link["tag_id"] for link in links]
    tags = list(db.tags.find({"tag_id": {"$in": tag_ids}}))
    for t in tags: t["_id"] = str(t["_id"])
    return {"tags": tags}


@app.get("/authors/{author_name}/books")
def get_author_books(author_name: str):
    books = list(db.books.find({"authors": {"$regex": author_name, "$options": "i"}}))
    for b in books: b["_id"] = str(b["_id"])
    return {"books": books, "total": len(books)}

@app.get("/tags")
def list_tags(page: int = 1, page_size: int = Query(20, le=100)):
    total = db.tags.count_documents({})
    tags = list(db.tags.find().skip((page-1)*page_size).limit(page_size))
    for t in tags: t["_id"] = str(t["_id"])
    for t in tags:
        count = db.book_tags.count_documents({"tag_id": t["tag_id"]})
        t["book_count"] = count
    return {"items": tags, "page": page, "page_size": page_size, "total": total}

@app.get("/users/{user_id}/to-read")
def get_user_to_read(user_id: int):
    links = db.to_read.find({"user_id": user_id})
    book_ids = [l["book_id"] for l in links]
    books = list(db.books.find({"book_id": {"$in": book_ids}}))
    for b in books: b["_id"] = str(b["_id"])
    return {"to_read": books}

@app.get("/books/{book_id}/ratings/summary")
def get_ratings_summary(book_id: int):
    ratings = list(db.ratings.find({"book_id": book_id}))
    if not ratings:
        return {"average": 0, "count": 0, "histogram": {str(i):0 for i in range(1,6)}}
    values = [r["rating"] for r in ratings]
    avg = sum(values)/len(values)
    count = len(values)
    hist = dict(Counter(values))
    for i in range(1,6):
        hist.setdefault(i,0)
    return {"average": round(avg,2), "count": count, "histogram": hist}

@app.post("/ratings", dependencies=[Depends(require_key)])
def upsert_rating(r: RatingIn):
    res = db.ratings.update_one(
        {"user_id": r.user_id, "book_id": r.book_id},
        {"$set": r.model_dump()},
        upsert=True
    )
    return {"upserted": bool(res.upserted_id), "matched": res.matched_count}
@app.get("/")
def root():
    return {"message": "GoodBooks API is running! Visit /docs for documentation."}
