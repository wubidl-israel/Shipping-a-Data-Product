from fastapi import HTTPException
from fastapi.responses import JSONResponse
from fastapi.requests import Request


class NotFoundException(Exception):
    def __init__(self, detail: str):
        self.detail = detail


class EmptyQueryException(Exception):
    def __init__(self, detail: str = "Query string must not be empty."):
        self.detail = detail


async def not_found_handler(request: Request, exc: NotFoundException):
    return JSONResponse(
        status_code=404,
        content={"error": "Not Found", "detail": exc.detail},
    )


async def empty_query_handler(request: Request, exc: EmptyQueryException):
    return JSONResponse(
        status_code=422,
        content={"error": "Invalid Query", "detail": exc.detail},
    )
