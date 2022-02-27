defmodule Jetdb.Query do
  import Jetdb.Table
  import Jetdb.Rows

  defp find_table(conn, table) do
    Enum.find(conn.schema, fn x -> Enum.at(Enum.at(x, 0), 2) == table end)
  end

  defp select(conn, table, columns, with_index, for_rows) do
    schema_table = find_table(conn, table)

    if is_nil(schema_table) do
      {:error, "table #{table} not found"}
    else
      tdef = Enum.at(schema_table, 1)
      select_columns = Enum.filter(tdef[:columns], fn x -> x[:name] in columns end)
      rows = case for_rows do
        [] ->
          used_pages_map = used_pages_map(conn.data_file, tdef[:used_pages_page])
          case with_index do
            false -> read_rows(conn.data_file, used_pages_map, select_columns)
            true -> read_rows_with_index(conn.data_file, used_pages_map, select_columns)
          end
        _ ->
          read_rows_for_index(conn.data_file, for_rows, select_columns)
      end
      {:ok, rows}
    end
  end

  def query(conn, type, table, columns, rows \\ []) do
    case type do
      :select ->
        select(conn, table, columns, false, rows)

      :index ->
        select(conn, table, columns, true, rows)

      _ ->
        {:error, "unknown query type"}
    end
  end
end
