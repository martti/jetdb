defmodule Jetdb.Connection do
  defstruct data_file: nil, schema: []

  def from_file(filename) do
    {:ok, data_file} = Jetdb.File.from_file(filename)
    schema = Jetdb.Schema.read_schema(data_file)

    {:ok,
     %Jetdb.Connection{
       data_file: data_file,
       schema: schema
     }}
  end
end
