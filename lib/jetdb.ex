defmodule Jetdb do
  import Jetdb.Schema

  def open do
    data_file = "testdata/data/nwind.mdb"
    {:ok, connection} = Jetdb.File.open(data_file)
    connection = read_schema(connection)
    orders = Jetdb.Query.query(connection, :select, "Orders", ["OrderID", "ShipName"])
    Enum.map(orders, &Enum.at(&1, 1))
  end
end
