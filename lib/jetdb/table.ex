defmodule Jetdb.Table do
  import Bitwise

  defp parse_cols(
         3,
         <<
           len::size(8)-unsigned-integer-little,
           rest::binary
         >>,
         cols
       )
       when cols > 0 do
    <<col::size(len)-bytes, rest::binary>> = rest
    [col | parse_cols(3, rest, cols - 1)]
  end

  defp parse_cols(3, <<_::binary>>, 0) do
    []
  end

  defp parse_cols(3, <<>>, 0) do
    []
  end

  defp parse_cols(
         4,
         <<
           len::size(16)-unsigned-integer-little,
           rest::binary
         >>,
         cols
       )
       when cols > 0 do
    <<col::size(len)-bytes, rest::binary>> = rest
    # IO.puts(:iconv.convert("ucs-2le", "utf-8", col))
    [{len, :iconv.convert("ucs-2le", "utf-8", col)} | parse_cols(4, rest, cols - 1)]
  end

  defp parse_cols(4, <<_::binary>>, 0) do
    []
  end

  defp parse_cols(4, <<>>, 0) do
    []
  end

  # jetdb3
  def parse_tdef(%Jetdb.File{version: 3}, <<
        0x02,
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

    column_names = parse_cols(3, col_names, num_cols)
    # IO.inspect(column_names)

    columns =
      for {a, b} <- Enum.zip(column_props, column_names) do
        a ++ [name: b]
      end

    # IO.inspect(columns)

    index_bytes = 39 * num_real_idx
    <<_::size(index_bytes)-bytes, rest::binary>> = col_names

    # IO.puts("num_idx: #{num_idx}")
    index_bytes = 20 * num_idx
    <<_::size(index_bytes)-bytes, _rest::binary>> = rest

    # _rest =
    #   if num_idx > 0 do
    #     idx_names = parse_cols(3, rest, num_idx)
    #     # IO.inspect(idx_names)
    #     total = Enum.reduce(idx_names, 0, fn col, acc -> 1 + byte_size(col) + acc end)
    #     # end
    #     # IO.puts("total #{total}")
    #     <<_::size(total)-bytes, rest::binary>> = rest
    #     rest
    #   else
    #     rest
    #   end

    # iterate if pages_col_num != 65535
    # IO.inspect(rest)
    # parsed_free_pages = parse_free_pages(rest)
    parsed_free_pages = []

    [
      used_pages_page: used_pages_page,
      used_pages_row: used_pages_row,
      columns: columns,
      indexes: indexes,
      free_pages: parsed_free_pages
    ]
  end

  # jetdb4
  def parse_tdef(%Jetdb.File{version: 4}, <<
        0x02,
        # unknown
        _::size(8),
        _free_space_in_page::size(16)-unsigned-integer-little,
        _next_page::size(32)-unsigned-integer-little,
        _tdef_len::size(32)-unsigned-integer-little,
        # unknown
        _::size(32)-unsigned-integer-little,
        _num_rows::size(32)-unsigned-integer-little,
        _auto_number::size(32)-unsigned-integer-little,
        _auto_number_flag::size(8)-unsigned-integer-little,
        # unknown
        _::size(24)-unsigned-integer-little,
        _auto_number_value::size(32)-unsigned-integer-little,
        # unknown
        _::size(64)-unsigned-integer-little,
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
      for <<_::size(32)-unsigned-integer-little,
            num_idx_rows_maybe::size(32)-unsigned-integer-little,
            _::size(32)-unsigned-integer-little <- binary_part(rest, 0, num_real_idx * 12)>>,
          do: {num_idx_rows_maybe}

    column_props =
      for <<
            col_type::size(8),
            # unknown
            _::size(32)-unsigned-integer-little,
            col_number::size(16)-unsigned-integer-little,
            offset_v::size(16)-unsigned-integer-little,
            _col_num::size(16)-unsigned-integer-little,
            _misc::size(16)-unsigned-integer-little,
            _misc_ext::size(16)-unsigned-integer-little,
            bitmask::size(8)-unsigned-integer-little,
            _misc_flags::size(8)-unsigned-integer-little,
            # unknown
            _::size(32)-unsigned-integer-little,
            offset_f::size(16)-unsigned-integer-little,
            col_len::size(16)-unsigned-integer-little <-
              binary_part(rest, num_real_idx * 12, num_cols * 25)
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

    sizex = num_real_idx * 12 + num_cols * 25
    <<_::size(sizex)-binary, col_names::binary>> = rest

    # IO.inspect(col_names)
    column_names = parse_cols(4, col_names, num_cols)

    columns =
      for {a, b} <- Enum.zip(column_props, column_names) do
        a ++ [name: elem(b, 1)]
      end

    # 2 + column length for each column
    columns_length = Enum.reduce(column_names, 0, &(&2 + elem(&1, 0) + 2))
    <<_::size(columns_length)-bytes, rest::binary>> = col_names

    # real index block 30 + 22 for each num_real_index
    index_bytes = (30 + 22) * num_real_idx
    <<real_index_block::size(index_bytes)-bytes, rest::binary>> = rest
    # IO.puts(Hexdump.to_string(real_index_block))

    # physical indexes
    real_indexes =
      for <<
        _::size(4)-bytes,
        column_block::size(30)-bytes,
        index_block::size(18)-bytes <- real_index_block
      >> do
        cols = for <<
          col_num::size(16)-unsigned-integer-little,
          col_order::size(8)-unsigned-integer-little <- column_block
        >> do
          [col_num: col_num, col_order: col_order]
        end
        <<
          used_pages_row::size(8)-unsigned-integer-little,
          used_pages_page::size(24)-unsigned-integer-little,
          first_dp::size(32)-unsigned-integer-little,
          flags::size(8)-unsigned-integer-little,
          _::size(9)-bytes
        >> = index_block
        [
        used_pages_page: used_pages_page,
        used_pages_row: used_pages_row,
        first_dp: first_dp,
        flags: flags,
        cols: cols,
        unique: (flags &&& 0x01) == 1,
        ignore_nuls: (flags &&& 0x02) == 2,
        required: (flags &&& 0x08) == 8
        ]
    end

    # index block 28 for each num_idx
    index_bytes = 28 * num_idx
    <<index_block::size(index_bytes)-bytes, rest::binary>> = rest
    # IO.puts(Hexdump.to_string(index_block))

    # logical indexes, index_num2 point to physical
    indexes =
      for <<
        _::size(4)-bytes, # unknown
        index_num::size(32)-unsigned-integer-little,
        index_num2::size(32)-unsigned-integer-little,
        rel_tbl_type::size(8)-unsigned-integer-little,
        rel_idx_num::size(32)-unsigned-integer-little,
        rel_tbl_page::size(32)-unsigned-integer-little,
        cascade_ups::size(8)-unsigned-integer-little,
        cascade_dels::size(8)-unsigned-integer-little,
        index_type::size(8)-unsigned-integer-little <- index_block
      >> do
        [
        index_num: index_num,
        index_num2: index_num2,
        rel_tbl_type: rel_tbl_type,
        rel_idx_num: rel_idx_num,
        rel_tbl_page: rel_tbl_page,
        cascade_ups: cascade_ups,
        cascade_dels: cascade_dels,
        index_type: index_type,
        primary: (index_type == 1),
        foreign: (index_type == 2),
        ]
    end

    # names of index length + name
    index_names = parse_cols(4, rest, num_idx)
    index_length = Enum.reduce(index_names, 0, &(&2 + elem(&1, 0) + 2))
    <<_::size(index_length)-bytes, rest::binary>> = rest

    indexes =
      for {a, b} <- Enum.zip(indexes, index_names) do
        a ++ [name: elem(b, 1)]
      end

    # IO.inspect(index_names)
    # IO.puts(Hexdump.to_string(rest))

    # iterate if pages_col_num != 65535
    parsed_free_pages = parse_free_pages(rest)

    # exit(:shutdown)

    [
      used_pages_page: used_pages_page,
      used_pages_row: used_pages_row,
      columns: columns,
      real_indexes: real_indexes,
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
    # IO.inspect(bits)
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

  def extract_bytes_from_page(data_file, pages) do
    # list of pages containing maps
    pages = for(<<page::size(32)-unsigned-integer-little <- pages>>, do: page)

    Enum.with_index(pages, fn page_page, index ->
      pages_in_page = (data_file.page_size - 4) * 8
      page_start = index * pages_in_page

      if page_page > 0 do
        usage_map = Enum.at(data_file, page_page)
        <<_::size(4)-bytes, bitmap::binary>> = usage_map
        extract_bytes(bitmap) |> Enum.map(&(&1 + page_start))
      end
    end)
    |> Enum.filter(&(!is_nil(&1)))
    |> List.flatten()
  end

  # these should probably also handle when used_pages_row from tdef is not 0?
  def used_pages_map(data_file = %Jetdb.File{version: 3}, used_pages_page) do
    usage_map = Enum.at(data_file, used_pages_page)

    <<
      _::size(10)-bytes,
      first_page_applies::size(16)-unsigned-integer-little,
      _::binary
    >> = usage_map

    <<
      _::size(first_page_applies)-bytes,
      bitmap::binary
    >> = usage_map

    <<
      map_type::size(8)-unsigned-integer-little,
      bitmap::binary
    >> = bitmap

    if map_type == 0 do
      <<page_start::size(32)-unsigned-integer-little, bitmap::binary>> = bitmap
      extract_bytes(bitmap) |> Enum.map(&(&1 + page_start))
    else
      # map_type 1
      extract_bytes_from_page(data_file, bitmap)
    end
  end

  def used_pages_map(data_file = %Jetdb.File{version: 4}, used_pages_page) do
    usage_map = Enum.at(data_file, used_pages_page)

    <<
      _::size(14)-bytes,
      first_page_applies::size(16)-unsigned-integer-little,
      _::binary
    >> = usage_map

    <<
      _::size(first_page_applies)-bytes,
      bitmap::binary
    >> = usage_map

    <<
      map_type::size(8)-unsigned-integer-little,
      bitmap::binary
    >> = bitmap

    if map_type == 0 do
      <<page_start::size(32)-unsigned-integer-little, bitmap::binary>> = bitmap
      extract_bytes(bitmap) |> Enum.map(&(&1 + page_start))
    else
      # map_type 1
      extract_bytes_from_page(data_file, bitmap)
    end
  end
end
