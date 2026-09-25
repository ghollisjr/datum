"""Unit tests for datum's printer module.

Tests cover display width calculation, padding, text formatting/truncation,
and full table formatting — with emphasis on East Asian wide characters
(CJK, Korean, fullwidth) that occupy 2 terminal columns.
"""

import decimal
from datetime import datetime, date, time

import pytest

from datum import printer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _init_printer():
    """Initialize the printer module with default config for every test."""
    printer.initialize_module({
        "newline_replacement": "\\n",
        "tab_replacement": "\\t",
        "column_display_length": 0,  # 0 = no truncation
        "null_string": "NULL",
        "rows_to_print": 50,
    })


# ---------------------------------------------------------------------------
# display_width
# ---------------------------------------------------------------------------

class TestDisplayWidth:
    def test_ascii(self):
        assert printer.display_width("hello") == 5

    def test_empty(self):
        assert printer.display_width("") == 0

    def test_cjk_characters(self):
        # Each CJK char is 2 columns wide
        assert printer.display_width("漢字") == 4

    def test_korean(self):
        assert printer.display_width("한글") == 4

    def test_japanese_katakana(self):
        assert printer.display_width("カタカナ") == 8

    def test_mixed_ascii_and_cjk(self):
        # "Hi" = 2, "世界" = 4 → total 6
        assert printer.display_width("Hi世界") == 6

    def test_fullwidth_latin(self):
        # Ａ is fullwidth Latin, 2 columns
        assert printer.display_width("Ａ") == 2

    def test_numeric_coercion(self):
        # display_width calls str() on its argument
        assert printer.display_width(42) == 2

    def test_single_wide_char(self):
        assert printer.display_width("中") == 2


# ---------------------------------------------------------------------------
# pad_to_width
# ---------------------------------------------------------------------------

class TestPadToWidth:
    def test_ascii_padding(self):
        result = printer.pad_to_width("hi", 5)
        assert result == "hi   "
        assert printer.display_width(result) == 5

    def test_cjk_padding(self):
        # "漢字" has display_width 4, pad to 6 → 2 spaces
        result = printer.pad_to_width("漢字", 6)
        assert result == "漢字  "
        assert printer.display_width(result) == 6

    def test_mixed_padding(self):
        # "A漢" → 1 + 2 = 3, pad to 5 → 2 spaces
        result = printer.pad_to_width("A漢", 5)
        assert result == "A漢  "
        assert printer.display_width(result) == 5

    def test_no_padding_needed(self):
        result = printer.pad_to_width("abc", 3)
        assert result == "abc"

    def test_width_already_exceeded(self):
        # Should not strip — just return as-is
        result = printer.pad_to_width("abcdef", 3)
        assert result == "abcdef"

    def test_numeric_coercion(self):
        result = printer.pad_to_width(42, 5)
        assert result == "42   "


# ---------------------------------------------------------------------------
# text_formatter
# ---------------------------------------------------------------------------

class TestTextFormatter:
    def test_newline_replacement(self):
        assert printer.text_formatter("a\nb") == "a\\nb"

    def test_tab_replacement(self):
        assert printer.text_formatter("a\tb") == "a\\tb"

    def test_carriage_return_stripped(self):
        assert printer.text_formatter("a\rb") == "ab"

    def test_no_truncation_when_disabled(self):
        long_str = "x" * 200
        assert printer.text_formatter(long_str) == long_str

    def test_truncation_ascii(self):
        printer._config["column_display_length"] = 15
        result = printer.text_formatter("a" * 20)
        assert result == "a" * 10 + "[...]"
        assert len(result) == 15

    def test_truncation_cjk(self):
        """CJK truncation respects display width, not character count."""
        printer._config["column_display_length"] = 15
        # 10 CJK chars = 20 display columns, exceeds 15
        cjk_str = "漢" * 10
        result = printer.text_formatter(cjk_str)
        assert result.endswith("[...]")
        assert printer.display_width(result) <= 15

    def test_truncation_mixed(self):
        printer._config["column_display_length"] = 12
        # "Hello世界Test" → 5+4+4=13, exceeds 12
        result = printer.text_formatter("Hello世界Test")
        assert result.endswith("[...]")
        assert printer.display_width(result) <= 12

    def test_truncation_does_not_split_wide_char(self):
        """If a wide char would straddle the truncation boundary, skip it."""
        printer._config["column_display_length"] = 10
        # limit = 10 - 5 = 5 display cols for content
        # Need a string with display_width > 10 that has a wide char near the boundary
        # "ab漢漢漢漢" → 2 + 8 = 10... need > 10
        # "ab漢漢漢漢x" → 11, truncated to 5 display cols of content
        # a(1) b(1) 漢(2) → 4, next 漢 would be 6 > 5, skip it
        result = printer.text_formatter("ab漢漢漢漢x")
        assert result.endswith("[...]")
        assert printer.display_width(result) <= 10
        # Content part should be "ab漢" (width 4) since next 漢 would exceed limit
        assert result == "ab漢[...]"


# ---------------------------------------------------------------------------
# format_row
# ---------------------------------------------------------------------------

class TestFormatRow:
    def test_data_tuple(self):
        result = printer.format_row([5, 3], ("hello", "foo"))
        assert result == "| hello | foo |"

    def test_hline_passthrough(self):
        hline = "|-------+-----|"
        assert printer.format_row([5, 3], hline) == hline

    def test_cjk_cells_padded(self):
        # "漢字" is width 4, column width 6 → 2 spaces padding
        result = printer.format_row([6], ("漢字",))
        assert result == "| 漢字   |"
        # Verify the content area is 6 display columns
        inner = result[2:-2]  # strip "| " and " |"
        assert printer.display_width(inner) == 6

    def test_mixed_row(self):
        # Mix of ASCII and CJK in different columns
        result = printer.format_row([6, 4], ("hello ", "漢字"))
        # "hello " padded to 6 → "hello " (already 6)
        # "漢字" padded to 4 → "漢字" (already 4)
        assert result == "| hello  | 漢字 |"


# ---------------------------------------------------------------------------
# format_rows — full table formatting
# ---------------------------------------------------------------------------

class TestFormatRows:
    def test_ascii_only(self):
        col_names = ["name", "age"]
        rows = [("Alice", "30"), ("Bob", "25")]
        widths, formatted = printer.format_rows(col_names, rows)
        # Header row
        header_line = printer.format_row(widths, formatted[0])
        assert "name" in header_line
        assert "age" in header_line
        # Hline
        assert formatted[1].startswith("|")
        assert "+" in formatted[1]
        # Data rows
        assert len(formatted) == 4  # header + hline + 2 data rows

    def test_cjk_column_values(self):
        """CJK data should produce wider columns and aligned output."""
        col_names = ["city"]
        rows = [("東京",), ("NYC",)]  # Tokyo=4, NYC=3
        widths, formatted = printer.format_rows(col_names, rows)
        assert widths[0] == 4  # "東京" is the widest at display width 4
        # All formatted rows should produce lines of equal display width
        lines = [printer.format_row(widths, r) for r in formatted]
        line_widths = [printer.display_width(l) for l in lines
                       if not l.startswith("|--")]
        assert len(set(line_widths)) == 1  # all same width

    def test_cjk_column_names(self):
        """Wide chars in column names should be measured correctly."""
        col_names = ["名前"]  # display width 4
        rows = [("AB",)]
        widths, formatted = printer.format_rows(col_names, rows)
        assert widths[0] == 4  # header "名前" is wider than "AB"

    def test_korean_mixed(self):
        col_names = ["이름", "score"]
        rows = [("김철수", "100"), ("Jane", "95")]
        widths, formatted = printer.format_rows(col_names, rows)
        # "김철수" = 6, "이름" = 4 → col 0 width should be 6
        assert widths[0] == 6
        lines = [printer.format_row(widths, r) for r in formatted]
        data_lines = [l for l in lines if not l.startswith("|--")]
        dwidths = [printer.display_width(l) for l in data_lines]
        assert len(set(dwidths)) == 1

    def test_empty_rows(self):
        col_names = ["名前", "id"]
        widths, formatted = printer.format_rows(col_names, [])
        assert widths[0] == 4  # "名前" display width
        assert widths[1] == 2  # "id" display width
        assert len(formatted) == 2  # header + hline only

    def test_null_values(self):
        col_names = ["col"]
        rows = [(None,)]
        widths, formatted = printer.format_rows(col_names, rows)
        null_row = printer.format_row(widths, formatted[2])
        assert "NULL" in null_row

    def test_alignment_consistency(self):
        """Every row in a table with mixed ASCII/CJK should have the same display width."""
        col_names = ["name", "city", "score"]
        rows = [
            ("Alice", "東京", "100"),
            ("Bob", "서울특별시", "95"),
            ("Carol", "NYC", "88"),
            ("田中太郎", "大阪", "77"),
        ]
        widths, formatted = printer.format_rows(col_names, rows)
        lines = [printer.format_row(widths, r) for r in formatted]
        data_lines = [l for l in lines if not l.startswith("|--")]
        dwidths = [printer.display_width(l) for l in data_lines]
        assert len(set(dwidths)) == 1, f"Misaligned widths: {dwidths}"

    def test_hline_width_matches_data(self):
        """The hline separator should have exactly the same character width as data rows."""
        col_names = ["名前", "score"]
        rows = [("田中", "100"), ("Kim", "95")]
        widths, formatted = printer.format_rows(col_names, rows)
        lines = [printer.format_row(widths, r) for r in formatted]
        hline = [l for l in lines if l.startswith("|--")][0]
        data_line = [l for l in lines if not l.startswith("|--")][0]
        # hline is pure ASCII so len == display_width
        assert len(hline) == printer.display_width(data_line)


# ---------------------------------------------------------------------------
# Per-type formatters
# ---------------------------------------------------------------------------

class TestFmtStr:
    def test_ascii(self):
        val, length = printer._fmt_str("hello")
        assert val == "hello"
        assert length == 5

    def test_cjk(self):
        val, length = printer._fmt_str("漢字")
        assert val == "漢字"
        assert length == 4  # display_width, not len

    def test_mixed(self):
        val, length = printer._fmt_str("Hi世界")
        assert val == "Hi世界"
        assert length == 6


class TestFmtBytes:
    def test_basic(self):
        val, length = printer._fmt_bytes(b'\xde\xad')
        assert val == "0xdead"
        assert length == 6


class TestFmtBool:
    def test_true(self):
        val, length = printer._fmt_bool(True)
        assert val is True
        assert length == 6


class TestFmtInt:
    def test_positive(self):
        val, length = printer._fmt_int(42)
        assert val == 42
        assert length == 2

    def test_zero(self):
        val, length = printer._fmt_int(0)
        assert val == 0
        assert length == 1

    def test_negative(self):
        val, length = printer._fmt_int(-5)
        assert val == -5
        assert length == 2


class TestFmtDecimal:
    def test_basic(self):
        val, length = printer._fmt_decimal(decimal.Decimal("3.14"))
        assert length == printer.decimal_len(decimal.Decimal("3.14"))


class TestFmtDate:
    def test_basic(self):
        val, length = printer._fmt_date(date(2024, 1, 15))
        assert val == "2024-01-15"
        assert length == 10


class TestFmtIsoformat:
    def test_datetime(self):
        dt = datetime(2024, 1, 15, 13, 30, 0)
        val, length = printer._fmt_isoformat(dt)
        assert val == dt.isoformat()
        assert length == len(val)

    def test_time(self):
        t = time(13, 30, 0)
        val, length = printer._fmt_isoformat(t)
        assert val == t.isoformat()
        assert length == len(val)


# ---------------------------------------------------------------------------
# int_len / decimal_len
# ---------------------------------------------------------------------------

class TestIntLen:
    def test_single_digit(self):
        assert printer.int_len(5) == 1

    def test_multi_digit(self):
        assert printer.int_len(123) == 3

    def test_zero(self):
        assert printer.int_len(0) == 1

    def test_negative(self):
        assert printer.int_len(-42) == 3  # "-42"


class TestKoreanQueryAlignment:
    """Regression test: SELECT '그래븜바이더프씨', 'hello' should produce aligned columns."""

    def test_korean_ascii_alignment(self):
        col_names = ["?column?", "?column?"]
        rows = [("그래븜바이더프씨", "hello")]
        widths, formatted = printer.format_rows(col_names, rows)

        lines = [printer.format_row(widths, r) for r in formatted]
        # Separate hline from data lines
        hline = [l for l in lines if l.startswith("|--")][0]
        data_lines = [l for l in lines if not l.startswith("|--")]

        # All data lines must have the same display width
        dwidths = [printer.display_width(l) for l in data_lines]
        assert len(set(dwidths)) == 1, f"Misaligned data rows: {dwidths}"

        # Hline (pure ASCII) must match data row display width
        assert len(hline) == dwidths[0], (
            f"Hline width {len(hline)} != data row width {dwidths[0]}\n"
            + "\n".join(lines)
        )

    def test_korean_ascii_printed_output(self):
        """Verify the actual formatted lines look correct."""
        col_names = ["?column?", "?column?"]
        rows = [("그래븜바이더프씨", "hello")]
        widths, formatted = printer.format_rows(col_names, rows)

        lines = [printer.format_row(widths, r) for r in formatted]
        for line in lines:
            print(line)

        # Korean string has display width 16, header is 8 → col 0 width = 16
        assert widths[0] == 16
        # "hello" is 5, header is 8 → col 1 width = 8
        assert widths[1] == 8

        # Check the | positions are consistent across all lines
        header, hline, data = lines[0], lines[1], lines[2]

        # Find column separator positions by display width in each line
        # Data rows use | and hline uses | and +
        def separator_positions(line):
            pos = 0
            seps = []
            for ch in line:
                if ch in ('|', '+'):
                    seps.append(pos)
                pos += 2 if printer.display_width(ch) == 2 else 1
            return seps

        header_seps = separator_positions(header)
        hline_seps = separator_positions(hline)
        data_seps = separator_positions(data)

        assert header_seps == hline_seps == data_seps, (
            f"Separator positions differ:\n"
            f"  header: {header_seps}\n"
            f"  hline:  {hline_seps}\n"
            f"  data:   {data_seps}\n"
            + "\n".join(lines)
        )


class TestDecimalLen:
    def test_with_decimals(self):
        assert printer.decimal_len(decimal.Decimal("3.14")) == 4

    def test_integer_as_decimal(self):
        # Oracle stores all numbers as Decimal, including integers
        assert printer.decimal_len(decimal.Decimal("42")) == 2

    def test_negative(self):
        assert printer.decimal_len(decimal.Decimal("-3.14")) == 5

    def test_large_capped_at_22(self):
        d = decimal.Decimal("1." + "0" * 30)
        assert printer.decimal_len(d) == 22
