from fastapi import FastAPI
from fastapi import Path
from api.crud import get_top_products, get_channel_activity, search_messages
from api.schemas import ObjectStat, ChannelActivity, MessageSearchResult, ChannelSlug
from api.exceptions import (
    NotFoundException,
    EmptyQueryException,
    not_found_handler,
    empty_query_handler,
)

# ______________ API Endpoints ______________#
app = FastAPI(
    title="Telegram Medical Insights",
    description="API for analysing message and image data from Ethiopian health Telegram channels.",
    version="1.0.0",
)

# Register exception handlers
app.add_exception_handler(NotFoundException, not_found_handler)
app.add_exception_handler(EmptyQueryException, empty_query_handler)


# ______________ Get top products ______________#
# This endpoint retrieves the top products based on mentions and confidence scores.
@app.get("/api/reports/top-products", response_model=list[ObjectStat])
def read_top_products(limit: int = 10):
    return get_top_products(limit)


# ______________ Get all channel slugs ______________#
# This endpoint retrieves all distinct channel slugs from the database.
@app.get(
    "/api/channels/{channel_slug}/activity",
    response_model=list[ChannelActivity],
    tags=["Channels"],
)
def read_channel_activity(channel_slug: ChannelSlug = Path(...)):
    activities = get_channel_activity(channel_slug.value)
    if not activities:
        raise NotFoundException(f"No activity found for channel: {channel_slug.value}")
    return activities


# ______________ Search messages ______________#
# This endpoint allows searching for messages containing a specific query string.
@app.get(
    "/api/search/messages", response_model=list[MessageSearchResult], tags=["Search"]
)
def read_search_messages(query: str):
    query = query.strip()
    if not query:
        raise EmptyQueryException()
    results = search_messages(query)
    if not results:
        raise NotFoundException(f"No messages found containing: '{query}'")
    return results
