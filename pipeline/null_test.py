import polars 
import pytest 

@pytest.fixture
def dataframe(file_path: str = "data/cleaned_github_issues.parquet") -> polars.DataFrame:
  data = polars.read_parquet(file_path)
  return data

def test_data_for_null(dataframe: polars.DataFrame) -> None:
  for col in dataframe.columns:
    assert dataframe[col].null_count() == 0
    
  return

if __name__ == "__main__":
  test_data_for_null(dataframe())