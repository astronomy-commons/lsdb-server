"""
This is just to test a python version of the server. This has shown to run 
at least 2x slower than the rust version. It consumes less of the CPU also. 

The rust version is the one that is currently being used.

Written by Gustavo Schwarz 
github.com/schwarzam
"""


from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import Response
from pathlib import Path
import polars as pl
import re
from io import BytesIO


app = FastAPI()

async def process_and_return_parquet_file(file_path: str, params: dict) -> bytes:
    df = pl.scan_parquet(file_path, hive_partitioning=False)

    # Processing the columns selection
    print(params)
    select_cols = params.get("cols", "").split(",") if params["cols"] is not None else []
    if select_cols:
        df = df.select([pl.col(col) for col in select_cols if col])

    # Processing the query conditions
    if "query" in params:
        conditions = [parse_condition(condition) for condition in params["query"].split(",")]
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition = combined_condition & condition
        
        df = df.filter(combined_condition)
        df = df.drop("_hipscat_index")

    # Collect the dataframe and convert to bytes
    df = df.collect()
    
    # Writing the dataframe to a buffer
    my_io = BytesIO()
    df.write_parquet(my_io)
    my_io.seek(0)
    return my_io.read()

def parse_condition(condition: str):
    re_pattern = r"([a-zA-Z_]+)([<>=]+)([0-9]+)"
    match = re.match(re_pattern, condition)
    if match:
        column, operator, value = match.groups()
        value = float(value)  # Assuming the value should be converted to float
        if operator == "<":
            return pl.col(column) < value
        elif operator == "<=":
            return pl.col(column) <= value
        elif operator == ">":
            return pl.col(column) > value
        elif operator == ">=":
            return pl.col(column) >= value
        elif operator == "=":
            return pl.col(column) == value
    raise ValueError("Unsupported operator or format in condition")

@app.get("/{file_path:path}")
async def catch_all(file_path: str, cols: str = Query(None), query: str = Query(None)):
    base_path = Path("/storage2/splus")
    full_path = base_path / file_path

    if not full_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    params = {"cols": cols, "query": query}
    bytes_data = await process_and_return_parquet_file(str(full_path), params)
    return Response(content=bytes_data, media_type="application/octet-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
