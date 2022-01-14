defmodule Jetdb.Rows do
  import Bitwise

  def read_rows(stream, tdef, used_pages_map) do
    for pagenumber <- used_pages_map do
      read_page = List.first(Enum.slice(stream, pagenumber, 1))

      # page_type
      <<
        _::size(8),
        # unknown
        _::size(8),
        free_space::size(16)-unsigned-integer-little,
        tdef_pg::size(32)-unsigned-integer-little,
        num_rows::size(16)-unsigned-integer-little,
        rest_of_page::binary
      >> = read_page

      IO.puts("page: #{pagenumber} -> free:#{free_space} tdef:#{tdef_pg} num_rows:#{num_rows}")
      IO.inspect(rest_of_page)

      offsets_size = num_rows * 2
      <<offsets::size(offsets_size)-bytes, _::binary>> = rest_of_page

      offset_map =
        for <<offset::size(16)-unsigned-integer-little <- offsets>> do
          #
          lookupflag = offset &&& 0x8000
          delflag = offset &&& 0x4000
          [offset &&& 0x1FFF, lookupflag, delflag]
        end

      # IO.puts("record: #{record_nr} offset: #{offset}")
      IO.inspect(offset_map)

      Enum.with_index(offset_map, fn offset, i ->
        if Enum.at(offset, 2) == 0 do
          length = if i == 0, do: 2048, else: Enum.at(Enum.at(offset_map, i - 1), 0)
          skip_bytes = Enum.at(offset, 0)
          IO.puts("skip: #{skip_bytes} length: #{length - skip_bytes - 1}")
          row_data = binary_part(read_page, skip_bytes, length - skip_bytes - 1)

          # skip_bytes = Enum.at(Enum.at(offset_map, 0), 0)
          # row_data = binary_part(read_page, skip_bytes, 2048-skip_bytes-1)
          # IO.inspect(row_data)
          <<_::size(skip_bytes)-bytes, num_cols_in_row::size(8)-unsigned-integer-little,
            _::binary>> = read_page

          # var len ennen null_mask size lopusta
          # binary_part(read_page)
          null_mask_size = div(num_cols_in_row + 7, 8)

          <<var_len::size(8)-unsigned-integer-little>> =
            binary_part(row_data, byte_size(row_data) - null_mask_size, 1)

          IO.puts("var_len: #{var_len}")

          skip = byte_size(row_data) - null_mask_size - 1 - var_len
          # <<_skip::size(skip)-bytes, var_table::size(var_len)-bytes, _rest::binary>> = row_data
          IO.puts("skip: #{skip}")

          var_table =
            binary_part(row_data, byte_size(row_data) - null_mask_size - var_len - 1, var_len + 1)

          var_offset_map =
            for(<<offset::size(8)-unsigned-integer-little <- var_table>>, do: offset)
            |> Enum.reverse()

          IO.puts("jumps: #{byte_size(row_data) / 256}")
          IO.inspect(var_table, binaries: :as_binaries)
          IO.inspect(var_offset_map, charlists: :as_lists)

          Enum.with_index(tdef[:columns], fn column, i ->
            if column[:is_fixed] do
              start = column[:offset_f] + 1

              value =
                case column[:type] do
                  4 ->
                    <<_::size(start)-bytes, value::size(32)-unsigned-integer-little, _::binary>> =
                      row_data

                    value

                  3 ->
                    <<_::size(start)-bytes, value::size(16)-unsigned-integer-little, _::binary>> =
                      row_data

                    value

                  _ ->
                    "column type #{column[:type]} not implemented"
                end

              IO.puts(
                "#{i}, #{column[:name]} #{column[:offset_f] + 1} -> #{column[:type]} : #{value}"
              )
            else
              var_offset = Enum.at(var_offset_map, column[:offset_v])
              size = Enum.at(var_offset_map, column[:offset_v] + 1) - var_offset

              value =
                case column[:type] do
                  10 ->
                    <<_::size(var_offset)-bytes, value::size(size)-bytes, _::binary>> = row_data
                    value

                  _ ->
                    "column type #{column[:type]} not implemented"
                end

              IO.puts("#{i}, #{column[:name]} #{var_offset} -> #{column[:type]} : #{value}")
            end
          end)
        end
      end)
    end
  end
end
