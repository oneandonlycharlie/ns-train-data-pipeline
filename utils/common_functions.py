from pyspark.sql import DataFrame
from delta.tables import DeltaTable
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


def merge_data(input_df, table_name, schema_name, merge_condition, spark):
  if (spark.catalog.tableExists(f"{schema_name}.{table_name}")):
    deltaTable = DeltaTable.forName(spark, f"{schema_name}.{table_name}")
    deltaTable.alias('target') \
    .merge(
      input_df.alias('source'),
      merge_condition
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll(
    ) \
    .execute()
  else:
    input_df.write.format("delta").mode("append").saveAsTable(f"{schema_name}.{table_name}")






