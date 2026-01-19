# Pydantic models for responses
from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional, List
from enum import Enum


# ______________ Channel Slugs ______________#
# Enum for predefined channel slugs to ensure consistency in API responses.
class ChannelSlug(str, Enum):
    CheMed123 = "CheMed123"
    lobelia4cosmetics = "lobelia4cosmetics"
    newoptics = "newoptics"
    ethiopianfoodanddrugauthority = "ethiopianfoodanddrugauthority"
    tikvahpharma = "tikvahpharma"
    yetenaweg = "yetenaweg"


# ______________ Object Statistics ______________#
# This model represents the statistics of detected objects in images.
class ObjectStat(BaseModel):
    object_class: str
    count: int
    avg_confidence: float


# ______________ Product Statistics ______________#
# This model represents the statistics of products mentioned in messages.
class ProductStat(BaseModel):
    product_name: str
    mention_count: int


# ______________ Channel Activity ______________#
# This model represents the daily activity of a channel, including message count and total views.
class ChannelActivity(BaseModel):
    date_day: date
    message_count: int
    total_views: int


# ______________ Message Search Result ______________#
# This model represents the result of a message search, including the message ID, channel slug, text, and posting date.


class Detection(BaseModel):
    object: str
    confidence: float


class MessageSearchResult(BaseModel):
    message_id: str
    channel_slug: str
    posted_at: datetime
    detections: List[Detection] = []
    text_preview: List[str]
