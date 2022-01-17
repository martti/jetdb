defmodule Jetdb.Table do
  import Bitwise

  defp parse_cols(
         <<
           len::size(8)-unsigned-integer-little,
           rest::binary
         >>,
         cols
       )
       when cols > 0 do
    <<col::size(len)-bytes, rest::binary>> = rest
    [col | parse_cols(rest, cols - 1)]
  end

  defp parse_cols(<<_::binary>>, 0) do
    []
  end

  defp parse_cols(<<>>, 0) do
    []
  end

  def parse_tdef(<<
        _::size(8),
        # unknown
        _::size(8),
        "VC",
        _next_page::size(32)-unsigned-integer-little,
        _tdef_len::size(32)-unsigned-integer-little,
        _num_rows::size(32)-unsigned-integer-little,
        _auto_number::size(32)-unsigned-integer-little,
        _table_type::size(8)-unsigned-integer-little,
        _max_cols::size(16)-unsigned-integer-little,
        _num_var_cols::size(16)-unsigned-integer-little,
        num_cols::size(16)-unsigned-integer-little,
        num_idx::size(32)-unsigned-integer-little,
        num_real_idx::size(32)-unsigned-integer-little,
        used_pages_row::size(8)-unsigned-integer-little,
        used_pages_page::size(24)-unsigned-integer-little,
        _free_pages::size(32)-unsigned-integer-little,
        rest::binary
      >>) do
    indexes =
      for <<unknown::size(32)-unsigned-integer-little,
            idxx::size(32)-unsigned-integer-little <- binary_part(rest, 0, num_real_idx * 8)>>,
          do: {unknown, idxx}

    column_props =
      for <<
            col_type::size(8),
            col_number::size(16)-unsigned-integer-little,
            offset_v::size(16)-unsigned-integer-little,
            _col_num::size(16)-unsigned-integer-little,
            _sort_order::size(16)-unsigned-integer-little,
            _misc::size(16)-unsigned-integer-little,
            _unknown::size(16)-unsigned-integer-little,
            bitmask::size(8)-unsigned-integer-little,
            offset_f::size(16)-unsigned-integer-little,
            col_len::size(16)-unsigned-integer-little <-
              binary_part(rest, num_real_idx * 8, num_cols * 18)
          >>,
          do: [
            type: col_type,
            number: col_number,
            offset_v: offset_v,
            offset_f: offset_f,
            length: col_len,
            is_fixed: (bitmask &&& 0x01) == 1,
            nullable: (bitmask &&& 0x02) == 2
          ]

    sizex = num_real_idx * 8 + num_cols * 18
    <<_::size(sizex)-binary, col_names::binary>> = rest

    column_names = parse_cols(col_names, num_cols)
    # IO.inspect(column_names)

    columns =
      for {a, b} <- Enum.zip(column_props, column_names) do
        a ++ [name: b]
      end

    IO.inspect(columns)

    index_bytes = 39 * num_real_idx
    <<_::size(index_bytes)-bytes, rest::binary>> = col_names

    # IO.puts("num_idx: #{num_idx}")
    index_bytes = 20 * num_idx
    <<_::size(index_bytes)-bytes, rest::binary>> = rest

    rest =
      if num_idx > 0 do
        idx_names = parse_cols(rest, num_idx)
        # IO.inspect(idx_names)
        total = Enum.reduce(idx_names, 0, fn col, acc -> 1 + byte_size(col) + acc end)
        # end
        # IO.puts("total #{total}")
        <<_::size(total)-bytes, rest::binary>> = rest
        rest
      else
        rest
      end

    # iterate if pages_col_num != 65535
    parsed_free_pages = parse_free_pages(rest)

    [
      used_pages_page: used_pages_page,
      used_pages_row: used_pages_row,
      columns: columns,
      indexes: indexes,
      free_pages: parsed_free_pages
    ]
  end

  defp extract_bytes(str) when is_binary(str) do
    extract_bytes(str, [], 0)
  end

  defp extract_bytes(<<byte::size(1)-bytes, bytes::binary>>, acc, page) do
    bits = Enum.reverse(for(<<x::size(1) <- byte>>, do: x))
    pages = for {x, c} <- Enum.with_index(bits), x == 1, do: page + c
    extract_bytes(bytes, acc ++ pages, page + 8)
  end

  defp extract_bytes(<<>>, acc, _page), do: acc

  def parse_free_pages(<<
        65535::size(16)-unsigned-integer-little,
        _used_pages::size(32)-unsigned-integer-little,
        _free_pages::size(32)-unsigned-integer-little,
        _rest::binary
      >>) do
    []
  end

  def parse_free_pages(<<
        pages_col_num::size(16)-unsigned-integer-little,
        used_pages::size(32)-unsigned-integer-little,
        free_pages::size(32)-unsigned-integer-little,
        rest::binary
      >>) do
    [pages_col_num, free_pages, used_pages | parse_free_pages(rest)]
  end

  def used_pages_map(stream, used_pages_page) do
    # IO.puts("used_pages_page: #{table_def.used_pages_page}")
    # IO.puts("used_pages_row: #{table_def.used_pages_row}")

    usage_map = List.first(Enum.slice(stream, used_pages_page, 1))

    <<
      _::size(10)-bytes,
      first_page_applies::size(16)-unsigned-integer-little,
      _::binary
    >> =
      usage_map

    <<
      _::size(first_page_applies)-bytes,
      bitmap::binary
    >> =
      usage_map

    IO.puts("first_page_applies: #{first_page_applies}")

    <<
      _::size(1)-bytes,
      _map_page_num::size(32)-unsigned-integer-little,
      bitmap::binary
    >> =
      bitmap

    extract_bytes(bitmap)
  end
end