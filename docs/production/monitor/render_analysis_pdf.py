"""Render the production Markdown report and its figures as a PDF artifact."""

from __future__ import annotations

import re
import subprocess
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure


ROOT_DIRECTORY = Path(__file__).resolve().parent
SOURCE_PATH = ROOT_DIRECTORY / "analysis.md"
OUTPUT_PATH = ROOT_DIRECTORY / "batchevict_latency_causal_analysis.pdf"

PAGE_WIDTH_INCHES = 8.27
PAGE_HEIGHT_INCHES = 11.69
REPORT_PAGE_SIZE = (PAGE_WIDTH_INCHES, PAGE_HEIGHT_INCHES)
A4_WIDTH_POINTS = 595.28
A4_HEIGHT_POINTS = 841.89
CONTENT_MARGIN_POINTS = 56.69
TOP_POSITION = 0.933
BOTTOM_POSITION = 0.067
LEFT_POSITION = 0.095

LINK_PATTERN = re.compile(r"\[([^]]+)]\(([^)]+)\)")
IMAGE_PATTERN = re.compile(r"!\[([^]]*)]\(([^)]+)\)")
TABLE_SEPARATOR_PATTERN = re.compile(r"^:?-{3,}:?$")
PDF_MEDIA_BOX_PATTERN = re.compile(
    rb"/MediaBox\s*\[\s*([-+0-9.]+)\s+([-+0-9.]+)\s+" rb"([-+0-9.]+)\s+([-+0-9.]+)\s*]"
)


def plain_inline(markdown_text: str) -> str:
    """Convert the inline Markdown used by this report to readable plain text."""

    linked_text = LINK_PATTERN.sub(r"\1", markdown_text)
    return linked_text.replace("`", "").replace("**", "")


def is_table_separator(row_cells: list[str]) -> bool:
    """Return whether every cell is a Markdown table alignment marker."""

    return bool(row_cells) and all(
        TABLE_SEPARATOR_PATTERN.fullmatch(cell.replace(" ", "")) for cell in row_cells
    )


def parse_table_row(markdown_line: str) -> list[str]:
    """Parse one pipe-delimited Markdown table row."""

    stripped_line = markdown_line.strip().strip("|")
    return [plain_inline(cell.strip()) for cell in stripped_line.split("|")]


@dataclass
class ReportPdfWriter:
    """Maintain text pagination and append figure and table pages."""

    pdf: PdfPages
    page_number: int = 0
    text_figure: Figure | None = None
    vertical_position: float = TOP_POSITION
    figure_replacements: list[tuple[int, Path]] = field(default_factory=list)

    def start_text_page(self) -> None:
        """Start a portrait text page."""

        self.page_number += 1
        self.text_figure = plt.figure(figsize=REPORT_PAGE_SIZE, facecolor="white")
        self.vertical_position = TOP_POSITION

    def finish_text_page(self) -> None:
        """Write and close the active text page."""

        active_figure = self.text_figure
        if active_figure is None:
            return
        active_figure.text(
            0.5,
            0.025,
            str(self.page_number),
            ha="center",
            va="center",
            fontsize=8,
            color="#64748B",
        )
        self.pdf.savefig(active_figure)
        plt.close(active_figure)
        self.text_figure = None

    def ensure_space(self, required_height: float) -> None:
        """Start a new page when the active page has insufficient space."""

        if self.text_figure is None:
            self.start_text_page()
        if self.vertical_position - required_height < BOTTOM_POSITION:
            self.finish_text_page()
            self.start_text_page()

    def add_line(
        self,
        line_text: str,
        *,
        font_size: float = 9.2,
        line_height: float = 0.021,
        weight: str = "normal",
        family: str = "sans-serif",
        color: str = "#172033",
        left_position: float = LEFT_POSITION,
    ) -> None:
        """Add one line of text to the active page."""

        self.ensure_space(line_height)
        active_figure = self.text_figure
        if active_figure is None:
            raise RuntimeError("Text page initialization failed")
        active_figure.text(
            left_position,
            self.vertical_position,
            line_text,
            ha="left",
            va="top",
            fontsize=font_size,
            fontweight=weight,
            fontfamily=family,
            color=color,
        )
        self.vertical_position -= line_height

    def add_paragraph(self, paragraph_text: str) -> None:
        """Add one wrapped body paragraph."""

        normalized_text = plain_inline(" ".join(paragraph_text.split()))
        wrapped_lines = textwrap.wrap(
            normalized_text,
            width=101,
            break_long_words=False,
            break_on_hyphens=False,
        )
        self.ensure_space(len(wrapped_lines) * 0.021 + 0.012)
        for wrapped_line in wrapped_lines:
            self.add_line(wrapped_line)
        self.vertical_position -= 0.010

    def add_heading(self, heading_text: str, level: int) -> None:
        """Add a report heading at the requested level."""

        heading_styles = {
            1: (18.0, 0.040, 48, "#173B57"),
            2: (13.0, 0.031, 76, "#173B57"),
            3: (10.8, 0.027, 88, "#2B526A"),
        }
        font_size, line_height, wrap_width, color = heading_styles[min(level, 3)]
        normalized_heading = plain_inline(" ".join(heading_text.split()))
        wrapped_headings = textwrap.wrap(
            normalized_heading,
            width=wrap_width,
            break_long_words=False,
            break_on_hyphens=False,
        )
        if level == 1 and self.text_figure is not None:
            self.finish_text_page()
        self.ensure_space(len(wrapped_headings) * line_height + 0.014)
        if level > 1:
            self.vertical_position -= 0.006
        for wrapped_heading in wrapped_headings:
            self.add_line(
                wrapped_heading,
                font_size=font_size,
                line_height=line_height,
                weight="bold",
                color=color,
            )
        self.vertical_position -= 0.008

    def add_bullet(self, bullet_text: str) -> None:
        """Add a wrapped bullet item."""

        normalized_text = plain_inline(" ".join(bullet_text.split()))
        wrapped_lines = textwrap.wrap(
            normalized_text,
            width=94,
            initial_indent="• ",
            subsequent_indent="   ",
            break_long_words=False,
            break_on_hyphens=False,
        )
        self.ensure_space(len(wrapped_lines) * 0.021 + 0.006)
        for wrapped_line in wrapped_lines:
            self.add_line(wrapped_line, left_position=0.10)
        self.vertical_position -= 0.004

    def add_code_block(self, code_lines: list[str]) -> None:
        """Add a monospaced code or formula block."""

        display_lines: list[str] = []
        for source_line in code_lines:
            wrapped_source_lines = textwrap.wrap(
                source_line,
                width=88,
                replace_whitespace=False,
                drop_whitespace=False,
            )
            display_lines.extend(wrapped_source_lines or [""])
        required_height = len(display_lines) * 0.020 + 0.018
        self.ensure_space(required_height)
        active_figure = self.text_figure
        if active_figure is None:
            raise RuntimeError("Text page initialization failed")
        block_top = self.vertical_position + 0.004
        block_height = len(display_lines) * 0.020 + 0.010
        active_figure.patches.append(
            plt.Rectangle(
                (0.075, block_top - block_height),
                0.85,
                block_height,
                transform=active_figure.transFigure,
                facecolor="#F1F5F9",
                edgecolor="#CBD5E1",
                linewidth=0.6,
            )
        )
        for display_line in display_lines:
            self.add_line(
                display_line,
                font_size=8.5,
                line_height=0.020,
                family="monospace",
                color="#263442",
                left_position=0.09,
            )
        self.vertical_position -= 0.012

    def add_image_page(self, image_path: Path, caption: str) -> None:
        """Reserve one page for replacement with the vector figure PDF."""

        self.finish_text_page()
        self.page_number += 1
        vector_figure_path = image_path.with_suffix(".pdf")
        if not vector_figure_path.is_file():
            raise FileNotFoundError(
                f"Vector figure is required for PDF output: {vector_figure_path}"
            )
        self.figure_replacements.append((self.page_number, vector_figure_path))
        placeholder_figure = plt.figure(figsize=REPORT_PAGE_SIZE, facecolor="white")
        placeholder_figure.text(
            0.5,
            0.5,
            plain_inline(caption),
            ha="center",
            va="center",
            fontsize=12,
        )
        self.pdf.savefig(placeholder_figure)
        plt.close(placeholder_figure)

    def add_table_page(self, table_rows: list[list[str]], caption: str) -> None:
        """Append one portrait page containing a formatted report table."""

        self.finish_text_page()
        self.page_number += 1
        column_count = len(table_rows[0])
        wrap_width = max(14, 84 // column_count)
        wrapped_rows = [
            [
                "\n".join(
                    textwrap.wrap(
                        cell,
                        width=wrap_width,
                        break_long_words=False,
                        break_on_hyphens=False,
                    )
                )
                for cell in row
            ]
            for row in table_rows
        ]
        table_figure, table_axis = plt.subplots(figsize=REPORT_PAGE_SIZE)
        table_axis.set_axis_off()
        report_table = table_axis.table(
            cellText=wrapped_rows[1:],
            colLabels=wrapped_rows[0],
            cellLoc="left",
            colLoc="left",
            loc="center",
        )
        report_table.auto_set_font_size(False)
        table_font_size = 6.7 if column_count > 4 else 8.0
        row_line_height = 0.016 if column_count > 4 else 0.022
        report_table.set_fontsize(table_font_size)
        for row_index, wrapped_row in enumerate(wrapped_rows):
            maximum_line_count = max(cell.count("\n") + 1 for cell in wrapped_row)
            row_height = 0.014 + maximum_line_count * row_line_height
            for column_index in range(column_count):
                report_table[(row_index, column_index)].set_height(row_height)
        for (row_index, _), table_cell in report_table.get_celld().items():
            table_cell.set_edgecolor("#CBD5E1")
            table_cell.set_linewidth(0.6)
            table_cell.set_text_props(va="center")
            if row_index == 0:
                table_cell.set_facecolor("#DCEAF2")
                table_cell.set_text_props(weight="bold", color="#173B57")
            elif row_index % 2 == 0:
                table_cell.set_facecolor("#F8FAFC")
        table_figure.suptitle(
            caption,
            fontsize=11,
            fontweight="bold",
            color="#173B57",
            y=0.945,
        )
        table_figure.text(
            0.5,
            0.018,
            str(self.page_number),
            ha="center",
            fontsize=8,
            color="#64748B",
        )
        self.pdf.savefig(table_figure)
        plt.close(table_figure)


def write_pdf_segment(
    input_paths: list[Path],
    output_path: Path,
    first_page: int | None = None,
    last_page: int | None = None,
) -> None:
    """Write vector PDF inputs to a fixed A4 portrait page size."""

    ghostscript_arguments = [
        "gs",
        "-q",
        "-dBATCH",
        "-dNOPAUSE",
        "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.7",
        "-dEmbedAllFonts=true",
        "-dFIXEDMEDIA",
        "-dPDFFitPage",
        "-dAutoRotatePages=/None",
        f"-dDEVICEWIDTHPOINTS={A4_WIDTH_POINTS}",
        f"-dDEVICEHEIGHTPOINTS={A4_HEIGHT_POINTS}",
        f"-sOutputFile={output_path}",
    ]
    if first_page is not None:
        ghostscript_arguments.append(f"-dFirstPage={first_page}")
    if last_page is not None:
        ghostscript_arguments.append(f"-dLastPage={last_page}")
    ghostscript_arguments.extend(str(input_path) for input_path in input_paths)
    subprocess.run(
        ghostscript_arguments,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def read_pdf_page_size(pdf_path: Path) -> tuple[float, float]:
    """Read the first uncompressed PDF media box emitted by Matplotlib."""

    pdf_bytes = pdf_path.read_bytes()
    media_box_match = PDF_MEDIA_BOX_PATTERN.search(pdf_bytes)
    if media_box_match is None:
        raise ValueError(f"PDF MediaBox is unavailable: {pdf_path}")
    x_minimum, y_minimum, x_maximum, y_maximum = (
        float(raw_coordinate) for raw_coordinate in media_box_match.groups()
    )
    return x_maximum - x_minimum, y_maximum - y_minimum


def write_vector_figure_page(input_path: Path, output_path: Path) -> None:
    """Fit one vector figure within 20 mm margins on an A4 portrait page."""

    source_width, source_height = read_pdf_page_size(input_path)
    content_width = A4_WIDTH_POINTS - 2.0 * CONTENT_MARGIN_POINTS
    content_height = A4_HEIGHT_POINTS - 2.0 * CONTENT_MARGIN_POINTS
    figure_scale = min(content_width / source_width, content_height / source_height)
    horizontal_offset = (A4_WIDTH_POINTS - source_width * figure_scale) / 2.0
    vertical_offset = (A4_HEIGHT_POINTS - source_height * figure_scale) / 2.0
    install_procedure = (
        "<</Install {"
        f"{horizontal_offset:.6f} {vertical_offset:.6f} translate "
        f"{figure_scale:.9f} {figure_scale:.9f} scale"
        "}>> setpagedevice"
    )
    ghostscript_arguments = [
        "gs",
        "-q",
        "-dBATCH",
        "-dNOPAUSE",
        "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.7",
        "-dEmbedAllFonts=true",
        "-dFIXEDMEDIA",
        "-dAutoRotatePages=/None",
        f"-dDEVICEWIDTHPOINTS={A4_WIDTH_POINTS}",
        f"-dDEVICEHEIGHTPOINTS={A4_HEIGHT_POINTS}",
        f"-sOutputFile={output_path}",
        "-c",
        install_procedure,
        "-f",
        str(input_path),
    ]
    subprocess.run(
        ghostscript_arguments,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def assemble_vector_report(
    intermediate_path: Path,
    output_path: Path,
    page_count: int,
    figure_replacements: list[tuple[int, Path]],
    temporary_directory: Path,
) -> None:
    """Replace figure placeholders with vector PDFs and concatenate all pages."""

    segment_paths: list[Path] = []
    next_intermediate_page = 1
    for replacement_index, (page_number, vector_path) in enumerate(figure_replacements):
        if next_intermediate_page < page_number:
            text_segment_path = temporary_directory / (
                f"text_segment_{replacement_index:02d}.pdf"
            )
            write_pdf_segment(
                [intermediate_path],
                text_segment_path,
                next_intermediate_page,
                page_number - 1,
            )
            segment_paths.append(text_segment_path)
        figure_segment_path = temporary_directory / (
            f"figure_segment_{replacement_index:02d}.pdf"
        )
        write_vector_figure_page(vector_path, figure_segment_path)
        segment_paths.append(figure_segment_path)
        next_intermediate_page = page_number + 1

    if next_intermediate_page <= page_count:
        final_text_segment_path = temporary_directory / "text_segment_final.pdf"
        write_pdf_segment(
            [intermediate_path],
            final_text_segment_path,
            next_intermediate_page,
            page_count,
        )
        segment_paths.append(final_text_segment_path)
    write_pdf_segment(segment_paths, output_path)


def render_intermediate_report(
    source_path: Path, intermediate_path: Path
) -> tuple[int, list[tuple[int, Path]]]:
    """Render Markdown with placeholders for vector figure pages."""

    source_lines = source_path.read_text(encoding="utf-8").splitlines()
    with PdfPages(
        intermediate_path,
        metadata={
            "Title": "BatchEvict Causal Assessment for KV-Transfer Latency and TTFT",
            "Author": "Mooncake Production Engineering",
            "Subject": "Production latency and BatchEvict analysis",
        },
    ) as report_pdf:
        writer = ReportPdfWriter(report_pdf)
        paragraph_lines: list[str] = []
        code_lines: list[str] = []
        table_lines: list[str] = []
        in_code_block = False
        latest_heading = "Production analysis"

        def flush_paragraph() -> None:
            if paragraph_lines:
                writer.add_paragraph(" ".join(paragraph_lines))
                paragraph_lines.clear()

        def flush_table() -> None:
            if not table_lines:
                return
            parsed_rows = [parse_table_row(line) for line in table_lines]
            content_rows = [row for row in parsed_rows if not is_table_separator(row)]
            writer.add_table_page(content_rows, latest_heading)
            table_lines.clear()

        for line_index, source_line in enumerate(source_lines):
            stripped_line = source_line.strip()
            if stripped_line.startswith("```"):
                flush_paragraph()
                flush_table()
                if in_code_block:
                    writer.add_code_block(code_lines)
                    code_lines.clear()
                in_code_block = not in_code_block
                continue
            if in_code_block:
                code_lines.append(source_line)
                continue

            image_match = IMAGE_PATTERN.fullmatch(stripped_line)
            if image_match:
                flush_paragraph()
                flush_table()
                image_caption, relative_path = image_match.groups()
                image_path = source_path.parent / relative_path
                writer.add_image_page(image_path, image_caption)
                continue

            if stripped_line.startswith("|") and stripped_line.endswith("|"):
                flush_paragraph()
                table_lines.append(stripped_line)
                continue
            flush_table()

            heading_match = re.match(r"^(#{1,3})\s+(.+)$", stripped_line)
            if heading_match:
                flush_paragraph()
                heading_marks, heading_text = heading_match.groups()
                latest_heading = plain_inline(heading_text)
                following_lines = source_lines[line_index + 1 :]
                following_content = next(
                    (line.strip() for line in following_lines if line.strip()), ""
                )
                heading_introduces_full_page_item = bool(
                    following_content.startswith("|")
                    or IMAGE_PATTERN.fullmatch(following_content)
                )
                if heading_introduces_full_page_item:
                    continue
                writer.add_heading(heading_text, len(heading_marks))
                continue

            if stripped_line.startswith("- "):
                flush_paragraph()
                writer.add_bullet(stripped_line[2:])
                continue
            if stripped_line.startswith("> "):
                flush_paragraph()
                writer.add_paragraph(stripped_line[2:])
                continue
            if not stripped_line:
                flush_paragraph()
                continue
            paragraph_lines.append(stripped_line)

        flush_paragraph()
        flush_table()
        writer.finish_text_page()
    return writer.page_number, writer.figure_replacements


def render_report(source_path: Path, output_path: Path) -> None:
    """Render one vector, fixed-page-size PDF from the Markdown report."""

    with TemporaryDirectory(prefix="mooncake-causal-report-") as temporary_name:
        temporary_directory = Path(temporary_name)
        intermediate_path = temporary_directory / "report_with_placeholders.pdf"
        page_count, figure_replacements = render_intermediate_report(
            source_path, intermediate_path
        )
        assemble_vector_report(
            intermediate_path,
            output_path,
            page_count,
            figure_replacements,
            temporary_directory,
        )


def main() -> None:
    """Generate the commit-ready PDF report."""

    render_report(SOURCE_PATH, OUTPUT_PATH)


if __name__ == "__main__":
    main()
