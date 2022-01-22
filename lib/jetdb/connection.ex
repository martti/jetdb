defmodule Jetdb.Connection do
  defstruct data_file: nil, page_count: 0, page_size: 2048, version: 3, schema: []
end
