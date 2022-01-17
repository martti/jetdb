defmodule Jetdb.Schema do
  import Jetdb.Table
  import Jetdb.Rows
  import Bitwise

  # check that table type is 1 and it is user table (flags not & 0x80000002)
  defp is_user_table(columns) do
    (Enum.at(columns, 3) &&& 0x00FFFFFF) == 1 && (Enum.at(columns, 7) &&& 0x80000002) == 0
  end

  defp parse_columns(columns) do
    [
      Enum.at(columns, 0) &&& 0x7F, # table definition page
      Enum.at(columns, 3) &&& 0x00FFFFFF, # type
      Enum.at(columns, 2), # name
      Enum.at(columns, 7) &&& 0x80000002 # flags
    ]
  end

  def read_schema(stream) do
    # schema is page 2
    schema_page = List.first(Enum.slice(stream, 2, 1))
    tdef = parse_tdef(schema_page)
    used_pages_map = used_pages_map(stream, tdef[:used_pages_page])
    rows = read_rows(stream, tdef, used_pages_map)
    tables = Enum.filter(rows, &is_user_table/1) |> Enum.map(&parse_columns/1)
    IO.inspect(tables)
    # should read table columns from every tdef
    # IO.inspect(List.first(List.first(rows)))
  end
end
