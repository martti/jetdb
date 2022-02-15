defmodule Jetdb.File do
  defstruct data_file: nil, page_count: 0, page_size: 2048, version: 3

  defimpl Enumerable do
    def count(file = %Jetdb.File{}), do: {:ok, file.page_count}
    def member?(file = %Jetdb.File{}, page), do: {:ok, page in 0..file.page_count}
    # maybe needs proper reduce functions
    def reduce(_file = %Jetdb.File{}, {:cont, acc}, _fun), do: {:done, acc}

    def slice(file = %Jetdb.File{}) do
      {:ok, file.page_count,
       fn start, length ->
         :file.position(file.data_file, start * file.page_size)
         {:ok, read_pages} = :file.read(file.data_file, file.page_size * length)
         page_size = file.page_size
         for <<chunk::size(page_size)-binary <- read_pages>>, do: chunk
       end}
    end
  end

  defp page_size(filename) do
    {_, content} = :file.open(filename, [:read, :binary])
    first_page = :file.read(content, 2048)
    :file.close(content)

    case first_page do
      # jetdb 3
      {:ok, <<_::size(160), 0x00, _::binary>>} -> {:ok, 2, 3}
      # jetdb 4
      {:ok, <<_::size(160), 0x01, _::binary>>} -> {:ok, 4, 4}
      _ -> {:error, "Unknown jetdb version"}
    end
  end

  defp check_file_length(filename, page_size) do
    %{size: size} = File.stat!(filename)

    if rem(size, page_size * 1024) != 0 do
      {:error, "File length is not consistent"}
    else
      {:ok, size}
    end
  end

  def from_file(filename) do
    {:ok, page_size, jetdb_version} = page_size(filename)
    {:ok, file_size} = check_file_length(filename, page_size)
    page_count = file_size / (page_size * 1024)
    {:ok, data_file} = :file.open(filename, [:read, :binary])

    {:ok,
     %Jetdb.File{
       data_file: data_file,
       page_count: page_count,
       page_size: page_size * 1024,
       version: jetdb_version
     }}
  end
end
