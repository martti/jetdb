defmodule Jetdb do
  import Jetdb.File
  import Jetdb.Schema

  def open do
    data_file = "testdata/data/nwind.mdb"
    {:ok, _page_count, _jetdb_version, stream} = open(data_file)
    schema = read_schema(stream)
    IO.inspect(schema)
  end
end
