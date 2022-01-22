defmodule Jetdb.File do
  defp page_size(filename) do
    {_, content} = :file.open(filename, [:read, :binary])
    first_page = :file.read(content, 2048)
    :file.close(content)

    case first_page do
      {:ok, <<_::size(152), 0x00, _::binary>>} -> {:ok, 2, 3} # jetdb 3
      {:ok, <<_::size(152), 0x01, _::binary>>} -> {:ok, 4, 4} # jetdb 4
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

  def open(filename) do
    {:ok, page_size, jetdb_version} = page_size(filename)
    {:ok, file_size} = check_file_length(filename, page_size)

    page_count = file_size / (page_size * 1024)
    {:ok, %Jetdb.Connection{
      data_file: File.stream!(filename, [], page_size * 1024),
      page_count: page_count,
      page_size: page_size * 1024,
      version: jetdb_version,
    }}
  end
end
