defmodule Jetdb.Schema do
  import Jetdb.Table
  import Jetdb.Rows
  import Bitwise

  # check that table type is 1 and it is user table (flags not & 0x80000002)
  defp is_user_table(columns, column_positions) do
    (Enum.at(columns, Enum.at(column_positions, 1)) &&& 0x00FFFFFF) == 1 &&
      (Enum.at(columns, Enum.at(column_positions, 3)) &&& 0x80000002) == 0
  end

  # is &&& 0x7F needed for table definition page?
  defp parse_columns(columns, column_positions) do
    [
      # table definition page
      Enum.at(columns, Enum.at(column_positions, 0)),
      # type
      Enum.at(columns, Enum.at(column_positions, 1)) &&& 0x00FFFFFF,
      # name
      Enum.at(columns, Enum.at(column_positions, 2)),
      # flags
      Enum.at(columns, Enum.at(column_positions, 3)) &&& 0x80000002
    ]
  end

  def read_schema(data_file) do
    # schema or catalog is page 2
    schema_page = Enum.at(data_file, 2)
    tdef = parse_tdef(data_file, schema_page)

    # read schema tdef column positions for Id, Type, Name, Flags
    column_positions = [
      Enum.find_index(tdef[:columns], &(&1[:name] == "Id")),
      Enum.find_index(tdef[:columns], &(&1[:name] == "Type")),
      Enum.find_index(tdef[:columns], &(&1[:name] == "Name")),
      Enum.find_index(tdef[:columns], &(&1[:name] == "Flags"))
    ]

    used_pages_map = used_pages_map(data_file, tdef[:used_pages_page])
    rows = read_rows(data_file, used_pages_map, tdef[:columns])

    tables =
      Enum.filter(rows, &is_user_table(&1, column_positions))
      |> Enum.map(&parse_columns(&1, column_positions))

    schema =
      Enum.map(tables, fn table ->
        table_page = Enum.at(data_file, Enum.at(table, 0))
        tdef = parse_tdef(data_file, table_page)
        [table, tdef]
      end)

    schema
  end
end
