import json
import os
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3


# ============================================================
# AWS
# ============================================================

s3 = boto3.client("s3")


# ============================================================
# Environment variables
# ============================================================

YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]
S3_BUCKET = os.environ["S3_BUCKET"]


# ============================================================
# Countries
#
# ISO 3166-1 alpha-2 country codes
# ============================================================

COUNTRIES = {
    "DZ": "Algeria",
    "BH": "Bahrain",
    "EG": "Egypt",
    "IQ": "Iraq",
    "IL": "Israel",
    "JO": "Jordan",
    "KW": "Kuwait",
    "LB": "Lebanon",
    "LY": "Libya",
    "MA": "Morocco",
    "OM": "Oman",
   # "PS": "Palestine", -- not supported by YouTube
    "QA": "Qatar",
    "SA": "Saudi Arabia",
   # "SY": "Syria", -- not supported by YouTube
    "TN": "Tunisia",
    "AE": "United Arab Emirates",
    "YE": "Yemen"
}


# ============================================================
# YouTube API helper
# ============================================================

def youtube_api_request(endpoint, params):
    """
    Make a GET request to the YouTube Data API.
    """

    params["key"] = YOUTUBE_API_KEY

    query_string = urllib.parse.urlencode(params)

    url = (
        f"https://www.googleapis.com/youtube/v3/"
        f"{endpoint}?{query_string}"
    )

    try:

        with urllib.request.urlopen(url, timeout=30) as response:

            response_body = response.read().decode("utf-8")

            return json.loads(response_body)

    except urllib.error.HTTPError as error:

        error_body = error.read().decode("utf-8")

        print(
            f"YouTube API error "
            f"{error.code}: {error_body}"
        )

        raise

    except urllib.error.URLError as error:

        print(
            f"Network error calling YouTube API: {error}"
        )

        raise


# ============================================================
# Get category mapping
# ============================================================

def get_category_mapping():

    """
    Retrieve YouTube video category IDs and names.

    Example:

    {
        "1": "Film & Animation",
        "10": "Music",
        "20": "Gaming",
        "24": "Entertainment"
    }
    """

    response = youtube_api_request(
        "videoCategories",
        {
            "part": "snippet",
            "regionCode": "US"
        }
    )

    category_mapping = {}

    for item in response.get("items", []):

        category_id = item["id"]

        category_title = (
            item
            .get("snippet", {})
            .get("title")
        )

        if category_title:
            category_mapping[category_id] = category_title

    return category_mapping


# ============================================================
# Get most popular videos for a country
# ============================================================

def get_most_popular_videos(country_code):

    """
    Retrieve up to 50 most-popular videos
    for a specific country.
    """

    response = youtube_api_request(
        "videos",
        {
            "part": (
                "snippet,"
                "contentDetails,"
                "statistics"
            ),

            "chart": "mostPopular",

            "regionCode": country_code,

            "maxResults": 50
        }
    )

    return response


# ============================================================
# Add country + category information
# ============================================================

def enrich_videos(api_response,
                country_code,
                country_name,
                category_mapping,
                ingestion_timestamp):

    enriched_videos = []

    for rank, video in enumerate(api_response.get("items", []), start=1):

        snippet = video.get("snippet", {})
        category_id = snippet.get("categoryId")
        category_name = category_mapping.get(category_id,"Unknown")

        enriched_video = {

            # ----------------------------------------------
            # Pipeline metadata
            # ----------------------------------------------

            "ingestion_timestamp":ingestion_timestamp,
            "country_code":country_code,
            "country_name":country_name,
            "chart":"mostPopular",
            "rank":rank,

            # ----------------------------------------------
            # Video identifiers
            # ----------------------------------------------

            "video_id":video.get("id"),
            "channel_id":snippet.get("channelId"),
            "channel_name":snippet.get("channelTitle"),

            # ----------------------------------------------
            # Video metadata
            # ----------------------------------------------

            "title":snippet.get("title"),
            "description":snippet.get("description"),
            "published_at":snippet.get("publishedAt"),
            "category_id":category_id,
            "category_name":category_name,

            # ----------------------------------------------
            # Content details
            # ----------------------------------------------

            "duration": video.get("contentDetails", {}).get("duration"),
            "definition": video.get("contentDetails", {}).get("definition"),
            "caption": video.get("contentDetails", {}).get("caption"),

            # ----------------------------------------------
            # Statistics
            # ----------------------------------------------

            "view_count": video.get("statistics", {}).get("viewCount"),
            "like_count": video.get("statistics", {}).get("likeCount"),
            "comment_count":video.get("statistics", {}).get("commentCount"),

            # ----------------------------------------------
            # Original raw API object
            #
            # IMPORTANT:
            # We preserve this because this is Bronze.
            # ----------------------------------------------

            "raw_api_response":video
        }

        enriched_videos.append(enriched_video)

    return enriched_videos


# ============================================================
# Write data to S3
# ============================================================

def write_to_s3(country_code,
                ingestion_date,
                ingestion_timestamp,
                videos):

    """
    Store the country's raw API response in S3 Bronze.
    """

    s3_key = (
        f"youtube/"
        f"most_popular/"
        f"country={country_code}/"
        f"date={ingestion_date}/"
        f"youtube_most_popular_"
        f"{country_code}_"
        f"{ingestion_date}.json"
    )


    bronze_data = {

        # ----------------------------------------------------
        # Pipeline metadata
        # ----------------------------------------------------

        "metadata": {

            "source":"youtube_data_api",
            "endpoint":"videos.list",
            "chart":"mostPopular",
            "country_code":country_code,
            "ingestion_timestamp":ingestion_timestamp,
            "ingestion_date":ingestion_date,
            "video_count":len(videos)
        },


        # ----------------------------------------------------
        # Videos
        # ----------------------------------------------------

        "videos":videos
    }


    json_data = json.dumps(
        bronze_data,
        ensure_ascii=False,
        indent=2
    )


    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json_data.encode("utf-8"),
        ContentType="application/json"
    )


    print(
        f"Uploaded: "
        f"s3://{S3_BUCKET}/{s3_key}"
    )


    return s3_key


# ============================================================
# Lambda handler
# ============================================================

def lambda_handler(event, context):

    # --------------------------------------------------------
    # Current UTC timestamp
    # --------------------------------------------------------

    now = datetime.now(timezone.utc)
    ingestion_timestamp = now.isoformat()
    ingestion_date = now.strftime("%Y-%m-%d")


    print(
        f"Starting YouTube ingestion: "
        f"{ingestion_timestamp}"
    )


    # --------------------------------------------------------
    # Get category mapping once
    #
    # We do NOT call videoCategories.list for every video.
    # --------------------------------------------------------

    print(
        "Retrieving YouTube video categories..."
    )

    category_mapping = get_category_mapping()

    print(
        f"Loaded "
        f"{len(category_mapping)} categories"
    )


    # --------------------------------------------------------
    # Results summary
    # --------------------------------------------------------

    successful_countries = []
    failed_countries = []
    total_videos = 0


    # --------------------------------------------------------
    # Process each country
    # --------------------------------------------------------

    for country_code, country_name in COUNTRIES.items():

        print(
            f"Processing "
            f"{country_name} "
            f"({country_code})..."
        )


        try:

            # ------------------------------------------------
            # Get most popular videos
            # ------------------------------------------------

            api_response = get_most_popular_videos(country_code)


            # ------------------------------------------------
            # Enrich videos
            # ------------------------------------------------

            videos = enrich_videos(api_response=api_response,
                                    country_code=country_code,
                                    country_name=country_name,
                                    category_mapping=category_mapping,
                                    ingestion_timestamp=ingestion_timestamp
                                    )


            # ------------------------------------------------
            # Write to Bronze
            # ------------------------------------------------

            s3_key = write_to_s3(country_code=country_code,
                                ingestion_date=ingestion_date,
                                ingestion_timestamp=ingestion_timestamp,
                                videos=videos)


            # ------------------------------------------------
            # Update results
            # ------------------------------------------------

            successful_countries.append(country_code)
            total_videos += len(videos)

            print(
                f"{country_name}: "
                f"{len(videos)} videos collected"
            )


        except Exception as error:

            print(
                f"ERROR processing "
                f"{country_name}: "
                f"{str(error)}"
            )

            failed_countries.append({"country_code":country_code,
                                    "country_name":country_name,
                                    "error":str(error)})


            # ------------------------------------------------
            # Continue to the next country 
            # ------------------------------------------------
            continue


    # --------------------------------------------------------
    # Final response
    # --------------------------------------------------------

    result = {
        "status":"completed",
        "ingestion_timestamp": ingestion_timestamp,
        "ingestion_date": ingestion_date,
        "countries_requested": len(COUNTRIES),
        "countries_successful": len(successful_countries),
        "countries_failed": len(failed_countries),
        "total_videos": total_videos,
        "successful_countries": successful_countries,
        "failed_countries": failed_countries
    }


    print(
        json.dumps(
            result,
            indent=2
        )
    )


    return {

        "statusCode":
            200,

        "body":
            json.dumps(result)
    }