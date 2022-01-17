defmodule Jetdb.Schema do
  import Jetdb.Table
  import Jetdb.Rows

  def read_schema(stream) do
    # schema is page 2
    schema_page = List.first(Enum.slice(stream, 2, 1))
    tdef = parse_tdef(schema_page)
    used_pages_map = used_pages_map(stream, tdef[:used_pages_page])
    read_rows(stream, tdef, used_pages_map)
  end
end
