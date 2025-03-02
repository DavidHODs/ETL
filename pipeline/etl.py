import asyncio
import os
from dotenv import load_dotenv
import httpx
import sentry_sdk
from typing import Any, Dict, List, cast
import os
import polars


load_dotenv(dotenv_path=".env")

sentry_sdk.init(os.getenv("SENTRY_DSN", ""), traces_sample_rate=1.0)

GITHUB_API_URL = "https://api.github.com/repos/pola-rs/polars/issues"

async def extract_data(page:int, per_page:int) -> List[Dict[str, Any]]:
  params = {"page": page, "per_page": per_page}
  headers = {"Accept": "application/vnd.github.v3+json"}
  
  async with httpx.AsyncClient() as client:
    try:
      response = await client.get(GITHUB_API_URL, params=params, headers=headers)
      response.raise_for_status()
      return cast(List[Dict[str, Any]], response.json())
    except Exception as err:
      sentry_sdk.capture_exception(err)
      return []
    
async def extract_all_data(total_pages: int, per_page: int = 100) -> List[Dict[str, Any]]:
  tasks = [extract_data(page, per_page) for page in range(1, total_pages + 1)]
  pages = await asyncio.gather(*tasks)
  return [record for page in pages for record in page]

def transform_data(data: List[Dict[str, Any]]) -> polars.DataFrame:
  fields = ["id", "title", "state", "reactions", "created_at", "updated_at"]
  selected_data = []
  
  for record in data:
    if record.get("title") is None or record.get("title") == "":
      record["title"] = "No Title"
    selected_record = {field: record.get(field) for field in fields}
    selected_data.append(selected_record)
    
    try:
      dataframe = polars.DataFrame(selected_data)
    except Exception as err:
      sentry_sdk.capture_exception(err)
      dataframe = polars.DataFrame([])
      
      return dataframe
  
def load_data_to_parquet(dataframe: polars.DataFrame, file_path: str) -> None:
  try:
    dataframe.write_parquet(file_path)
  except Exception as err:
    sentry_sdk.capture_exception(err)
    
async def run_etl_pipeline(total_pages: int, output_path: str) -> None:
  try:
    records = await extract_all_data(total_pages, per_page=100)
  except Exception as err:
    sentry_sdk.capture_exception(err)
    records = []
    
  try:
    dataframe = transform_data(records)
  except Exception as err:
    sentry_sdk.capture_exception(err)
    dataframe = polars.DataFrame([])
    
  load_data_to_parquet(dataframe, output_path)
  
if __name__ == "__main__":
  total_pages_to_fetch = 10
  output_path = "github_issues.parquet"
  asyncio.run(run_etl_pipeline(total_pages_to_fetch, output_path))