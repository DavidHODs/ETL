import polars

def inspect_parquet(file_path: str) -> None:
    dataframe = polars.read_parquet(file_path)
    
    print("\nShape:", dataframe.shape)
    print("\nColumns:", dataframe.columns)
    print("\nSchema:", dataframe.schema)
    print("Head of the DataFrame:\n")
    print(dataframe.head())

if __name__ == "__main__":
    parquet_file = "github_issues.parquet"
    inspect_parquet(parquet_file)
