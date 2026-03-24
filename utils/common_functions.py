from pyspark.sql import DataFrame
# TO DO - add test to function
def clean_column_names(df:DataFrame) -> DataFrame: 
  """
  Cleans column names by removing spaces and colons and replacing them with underscores.
  
  Args:
    df: A Spark DataFrame.
  
  Returns:
    A Spark DataFrame with cleaned column names.
  """
  column_list = df.columns
  for column in column_list:
    new_column_name = column. \
      replace(":", " ").replace(" ", "_").replace("-", "_").replace("__", "_") \
      .lower() # convert to snake case
    df = df.withColumnRenamed(column, new_column_name)
  return df