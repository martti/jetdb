defmodule Jetdb.Query do
  import Jetdb.Table
  import Jetdb.Rows

  defp find_table(conn, table) do
    Enum.find(conn.schema, fn x -> Enum.at(Enum.at(x, 0), 2) == table end)
  end

  defp select(conn, table, columns) do
    schema_table = find_table(conn, table)
    if is_nil(schema_table) do
      {:error, "table #{table} not found"}
    else
      tdef = Enum.at(schema_table, 1)
      used_pages_map = used_pages_map(conn.data_file, tdef[:used_pages_page])
      rows = read_rows(conn.data_file, tdef, used_pages_map)
      select_columns = Enum.filter(tdef[:columns], fn x -> x[:name] in columns end)
      {:ok, Enum.map(rows, fn x -> Enum.map(select_columns, &Enum.at(x, &1[:number])) end)}
    end
  end

  def query(conn, type, table, columns) do
    case type do
      :select ->
        select(conn, table, columns)
      _ ->
        {:error, "unknown query type"}
    end
  end
end
