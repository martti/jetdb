defmodule Jetdb do
  import Jetdb.Table
  import Jetdb.Rows

  def open do
    data_file = "testdata/data/nwind.mdb"

    {_, content} = :file.open(data_file, [:read, :binary])
    first_page = :file.read(content, 2048)
    :file.close(content)

    page_size =
      case first_page do
        {:ok, <<_::size(152), 0x00, _::binary>>} -> 2
        {:ok, <<_::size(152), 0x01, _::binary>>} -> 4
        _ -> raise "Unknown jetdb version"
      end

    %{size: size} = File.stat!(data_file)

    if rem(size, page_size * 1024) != 0 do
      raise "File length is not consistent"
    end

    page_count = size / (page_size * 1024)
    IO.puts("page_count: #{page_count}")
    stream = File.stream!(data_file, [], page_size * 1024)

    for tdef <- Enum.slice(stream, 2, 1) do
      tdef = parse_tdef(tdef)
      used_pages_map = used_pages_map(stream, tdef[:used_pages_page])

      read_rows(stream, tdef, used_pages_map)
    end
  end
end
