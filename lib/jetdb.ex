defmodule Jetdb do
  import Jetdb.File
  import Jetdb.Schema

  def open do
    data_file = "testdata/data/nwind.mdb"
    {:ok, _page_count, stream} = open(data_file)
    _schema = read_schema(stream)
  end
end
