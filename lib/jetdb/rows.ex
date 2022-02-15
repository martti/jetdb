defmodule Jetdb.Rows do
  import Bitwise

  # compressed
  defp parse_text(<<0xFF, 0xFE, rest::binary>>, _charset) do
    # :iconv.convert(charset, "utf-8", rest)
    # should handle where compression changes mid string (0x00)
    rest
  end

  defp parse_text(<<rest::binary>>, charset) do
    :iconv.convert(charset, "utf-8", rest)
  end

  defp parse_column(data_file, start, row_data, column, size) do
    case column[:type] do
      4 ->
        len = size * 8
        <<_::size(start)-bytes, value::size(len)-unsigned-integer-little, _::binary>> = row_data
        value

      3 ->
        len = size * 8
        <<_::size(start)-bytes, value::size(len)-unsigned-integer-little, _::binary>> = row_data
        value

      10 ->
        <<_::size(start)-bytes, value::size(size)-bytes, _::binary>> = row_data
        charset = if data_file.version == 3, do: "CP1252", else: "ucs-2le"
        parse_text(value, charset)

      12 ->
        # MDB_MEMO_OVERHEAD 12
        <<_::size(start)-bytes, memo_len::size(24)-unsigned-integer-little,
          memo_mask::size(8)-unsigned-integer-little, memo_row::size(8)-unsigned-integer-little,
          memo_page::size(24)-unsigned-integer-little, _::binary>> = row_data

        case memo_mask do
          # 0x80 inline
          0x80 ->
            pad_len = start + 12
            <<_::size(pad_len)-bytes, value::size(memo_len)-bytes, _::binary>> = row_data
            charset = if data_file.version == 3, do: "CP1252", else: "ucs-2le"
            parse_text(value, charset)

          # 0x40 # lval in page
          0x40 ->
            memo_data = Enum.at(data_file, memo_page)
            offset_map = parse_offset_map(data_file.version, memo_data)
            {_, offsets} = map_offsets(offset_map, data_file)
            [start_offset, _, _, end_offset] = Enum.at(offsets, memo_row)
            memo_size = end_offset - start_offset
            <<_::size(start_offset)-bytes, value::size(memo_size)-bytes, _::binary>> = memo_data
            charset = if data_file.version == 3, do: "CP1252", else: "ucs-2le"
            parse_text(value, charset)

          _ ->
            "t:#{column[:type]}:#{memo_mask}"
        end

      _ ->
        # "column type #{column[:type]} not implemented"
        "t:#{column[:type]}"
    end
  end

  defp parse_offset_map(3, read_page) do
    <<
      # page_type
      _::size(8),
      # unknown
      _::size(8),
      _free_space::size(16)-unsigned-integer-little,
      _tdef_pg::size(32)-unsigned-integer-little,
      num_rows::size(16)-unsigned-integer-little,
      rest_of_page::binary
    >> = read_page

    offsets_size = num_rows * 2
    <<offsets::size(offsets_size)-bytes, _::binary>> = rest_of_page

    for <<offset::size(16)-unsigned-integer-little <- offsets>>, do: offset
  end

  defp parse_offset_map(4, read_page) do
    <<
      # page_type
      _::size(8),
      # unknown
      _::size(8),
      _free_space::size(16)-unsigned-integer-little,
      _tdef_pg::size(32)-unsigned-integer-little,
      # unknown
      _::size(32)-unsigned-integer-little,
      num_rows::size(16)-unsigned-integer-little,
      rest_of_page::binary
    >> = read_page

    offsets_size = num_rows * 2
    <<offsets::size(offsets_size)-bytes, _::binary>> = rest_of_page

    for <<offset::size(16)-unsigned-integer-little <- offsets>>, do: offset
  end

  defp map_offsets(offset_map, data_file) do
    Enum.reduce(offset_map, {nil, []}, fn
      offset, {previous, offsets} ->
        previous = if is_nil(previous), do: data_file.page_size, else: previous
        lookupflag = offset &&& 0x8000
        delflag = offset &&& 0x4000
        offset = offset &&& 0x1FFF
        offset_info = [offset, lookupflag, delflag, previous]
        {offset, [offset_info | offsets]}
    end)
  end

  defp parse_row(data_file, pagenumber, columns) do
    read_page = Enum.at(data_file, pagenumber)
    offset_map = parse_offset_map(data_file.version, read_page)
    {_, offsets} = map_offsets(offset_map, data_file)

    Enum.map(Enum.filter(Enum.reverse(offsets), &(Enum.at(&1, 2) == 0)), fn offset ->
      num_cols_bytes = if data_file.version == 3, do: 8, else: 16
      var_len_size = if data_file.version == 3, do: 1, else: 2

      length = Enum.at(offset, 3)
      skip_bytes = Enum.at(offset, 0)
      row_data = binary_part(read_page, skip_bytes, length - skip_bytes)

      <<_::size(skip_bytes)-bytes, num_cols_in_row::size(num_cols_bytes)-unsigned-integer-little,
        _::binary>> = read_page

      null_mask_size = div(num_cols_in_row + 7, 8)

      <<var_len::size(num_cols_bytes)-unsigned-integer-little>> =
        binary_part(row_data, byte_size(row_data) - null_mask_size - var_len_size, var_len_size)

      var_table =
        binary_part(
          row_data,
          byte_size(row_data) - null_mask_size - var_len * var_len_size - var_len_size * 2,
          var_len * var_len_size + var_len_size
        )

      var_offset_map =
        for(<<offset::size(num_cols_bytes)-unsigned-integer-little <- var_table>>, do: offset)
        |> Enum.reverse()

      Enum.with_index(columns, fn column, _i ->
        if column[:is_fixed] do
          start = column[:offset_f] + var_len_size
          parse_column(data_file, start, row_data, column, column[:length])
        else
          var_offset = Enum.at(var_offset_map, column[:offset_v])
          next_offset = Enum.at(var_offset_map, column[:offset_v] + 1)
          size = if next_offset, do: next_offset - var_offset, else: 0
          if size > 0, do: parse_column(data_file, var_offset, row_data, column, size), else: ""
        end
      end)
    end)
    |> Enum.filter(&(!is_nil(&1)))
  end

  def read_rows(data_file, tdef, used_pages_map) do
    Enum.flat_map(used_pages_map, &parse_row(data_file, &1, tdef[:columns]))
  end
end
