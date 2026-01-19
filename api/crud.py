# SQL query logic
from api.database import get_connection


# ______________ Get all channel slugs ______________#
def get_all_channel_slugs():
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT DISTINCT channel_slug
        FROM raw_marts.fct_messages
        ORDER BY channel_slug;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return [row[0] for row in rows]


# ______________ Get top products ______________#
# This function retrieves the top products based on the number of mentions and average confidence score.
def get_top_products(limit=10):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT 
            detected_object,
            COUNT(*) AS count,
            ROUND(AVG(confidence_score)::numeric, 3) AS avg_confidence
        FROM enriched.fct_image_detections
        GROUP BY detected_object
        ORDER BY count DESC
        LIMIT %s;
    """
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return [
        {"object_class": row[0], "count": row[1], "avg_confidence": row[2]}
        for row in rows
    ]


# ______________ Get channel activity ______________#
# This function retrieves the daily message count and view count for a specific channel.
def get_channel_activity(channel_slug: str):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT 
            date_day,
            COUNT(*) AS message_count,
            SUM(COALESCE(views, 0)) AS total_views
        FROM raw_marts.fct_messages
        WHERE channel_slug = %s
        GROUP BY date_day
        ORDER BY date_day ASC;
    """
    cursor.execute(query, (channel_slug,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return [
        {"date_day": row[0], "message_count": row[1], "total_views": row[2]}
        for row in rows
    ]


# ______________ Search messages ______________#
# This function searches for messages containing a specific query string.
def format_text(raw_text):
    lines = raw_text.split("\n")
    lines = [line.strip() for line in lines if line.strip()]
    return lines[:15]  # limit to top 15 lines for brevity


def search_messages(query: str):
    conn = get_connection()
    cursor = conn.cursor()
    search = f"%{query.lower().strip()}%"
    print(f"Search pattern: {search}")
    sql = """
        SELECT 
            m.message_id,
            m.channel_slug,
            m.date_day AS posted_at,
            ARRAY_AGG(
                json_build_object(
                    'object', d.detected_object,
                    'confidence', ROUND(d.confidence_score::numeric, 3)
                )
            ) FILTER (WHERE d.detected_object IS NOT NULL) AS detections,
            m.text
        FROM raw_marts.fct_messages m
        LEFT JOIN enriched.fct_image_detections d
            ON m.message_id = d.message_id
        WHERE m.text IS NOT NULL AND LOWER(m.text) LIKE %s
        GROUP BY m.message_id, m.channel_slug, m.date_day, m.text
        ORDER BY posted_at DESC
        LIMIT 50;
        """

    try:
        cursor.execute(sql, (search,))
        rows = cursor.fetchall()
        print(f"Query returned {len(rows)} rows")  # Debug line
    except Exception as e:
        print(f"Search query failed: {e}")
        rows = []

    cursor.close()
    conn.close()

    results = []
    for row in rows:
        print("Row detections:", row[3])  # prints before return
        results.append(
            {
                "message_id": row[0],
                "channel_slug": row[1],
                "posted_at": row[2],
                "detections": row[3] or [],
                "text_preview": format_text(row[4]),
            }
        )

    return results
