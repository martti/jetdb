defmodule Jetdb do
  # import Jetdb.Schema

  def open do
    data_file = "testdata/data/nwind.mdb"
    {:ok, connection} = Jetdb.Connection.from_file(data_file)
    _orders = Jetdb.Query.query(connection, :select, "Orders", ["OrderID", "ShipName"])
    connection
  end
end
