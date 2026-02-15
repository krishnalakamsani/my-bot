#!/usr/bin/env python3

import argparse
import os
import re
from dataclasses import dataclass

from fpdf import FPDF


def _strip_inline_md(text: str) -> str:
    # Convert links: [text](url) -> text (url)
    text = re.sub(r"\[([^\]]+)\]\(([^\)]+)\)", r"\1 (\2)", text)
    # Remove images: ![alt](url)
    text = re.sub(r"!\[[^\]]*\]\([^\)]+\)", "", text)
    # Remove emphasis / code markers (best-effort)
    text = text.replace("**", "")
    text = text.replace("__", "")
    text = text.replace("*", "")
    text = text.replace("_", "")
    text = text.replace("`", "")
    return text.strip("\n")


@dataclass
class FontPaths:
    regular: str | None
    bold: str | None
    mono: str | None


def _find_dejavu_fonts() -> FontPaths:
    candidates = [
        "/usr/share/fonts/truetype/dejavu",
        "/usr/share/fonts/truetype",
        "/usr/share/fonts",
    ]
    regular = bold = mono = None
    for base in candidates:
        if not os.path.isdir(base):
            continue
        reg = os.path.join(base, "DejaVuSans.ttf")
        b = os.path.join(base, "DejaVuSans-Bold.ttf")
        m = os.path.join(base, "DejaVuSansMono.ttf")
        if regular is None and os.path.exists(reg):
            regular = reg
        if bold is None and os.path.exists(b):
            bold = b
        if mono is None and os.path.exists(m):
            mono = m
    return FontPaths(regular=regular, bold=bold, mono=mono)


class Pdf(FPDF):
    def __init__(self, title: str):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.doc_title = title

    def header(self):
        if self.page_no() == 1:
            return
        self.set_font("DejaVu", size=9)
        self.set_text_color(90, 90, 90)
        self.cell(0, 6, self.doc_title, new_x="LMARGIN", new_y="NEXT", align="L")
        self.set_text_color(0, 0, 0)
        self.ln(1)

    def footer(self):
        self.set_y(-12)
        self.set_font("DejaVu", size=9)
        self.set_text_color(90, 90, 90)
        self.cell(0, 10, f"Page {self.page_no()}", align="C")
        self.set_text_color(0, 0, 0)


def md_to_pdf(md_text: str, output_path: str, title: str) -> None:
    fonts = _find_dejavu_fonts()

    pdf = Pdf(title=title)
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_margins(left=15, top=15, right=15)

    # Fonts
    if fonts.regular:
        pdf.add_font("DejaVu", "", fonts.regular, uni=True)
    if fonts.bold:
        pdf.add_font("DejaVu", "B", fonts.bold, uni=True)
    if fonts.mono:
        pdf.add_font("DejaVuMono", "", fonts.mono, uni=True)

    if not fonts.regular:
        # Fallback (ASCII-ish only)
        pdf.set_font("Helvetica", size=11)

    pdf.add_page()
    if fonts.regular:
        pdf.set_font("DejaVu", "B" if fonts.bold else "", size=18)
    pdf.multi_cell(0, 10, title, wrapmode="CHAR")
    pdf.ln(2)

    in_code_block = False
    for raw_line in md_text.splitlines():
        line = raw_line.rstrip("\n")

        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            pdf.ln(2)
            continue

        # Blank line
        if not line.strip():
            pdf.ln(3)
            continue

        # Horizontal rule
        if re.fullmatch(r"\s*[-*_]{3,}\s*", line):
            y = pdf.get_y()
            pdf.line(pdf.l_margin, y, pdf.w - pdf.r_margin, y)
            pdf.ln(4)
            continue

        if in_code_block:
            if fonts.mono:
                pdf.set_font("DejaVuMono", size=9)
            elif fonts.regular:
                pdf.set_font("DejaVu", size=10)
            else:
                pdf.set_font("Helvetica", size=10)
            pdf.multi_cell(0, 4.5, line, wrapmode="CHAR")
            continue

        # Headings
        heading_match = re.match(r"^(#{1,6})\s+(.*)$", line)
        if heading_match:
            level = len(heading_match.group(1))
            text = _strip_inline_md(heading_match.group(2))
            size = {1: 16, 2: 14, 3: 12, 4: 11, 5: 11, 6: 11}.get(level, 11)
            if fonts.regular:
                pdf.set_font("DejaVu", "B" if fonts.bold else "", size=size)
            else:
                pdf.set_font("Helvetica", "B", size=size)
            pdf.multi_cell(0, 7, text, wrapmode="CHAR")
            pdf.ln(1)
            continue

        # Lists
        list_match = re.match(r"^\s*([-*])\s+(.*)$", line)
        numbered_match = re.match(r"^\s*(\d+)\.\s+(.*)$", line)
        if list_match:
            bullet = "•" if fonts.regular else "-"
            text = _strip_inline_md(list_match.group(2))
            if fonts.regular:
                pdf.set_font("DejaVu", size=11)
            else:
                pdf.set_font("Helvetica", size=11)
            pdf.multi_cell(0, 5.5, f"{bullet} {text}", wrapmode="CHAR")
            continue
        if numbered_match:
            n = numbered_match.group(1)
            text = _strip_inline_md(numbered_match.group(2))
            if fonts.regular:
                pdf.set_font("DejaVu", size=11)
            else:
                pdf.set_font("Helvetica", size=11)
            pdf.multi_cell(0, 5.5, f"{n}. {text}", wrapmode="CHAR")
            continue

        # Normal paragraph
        text = _strip_inline_md(line)
        if fonts.regular:
            pdf.set_font("DejaVu", size=11)
        else:
            pdf.set_font("Helvetica", size=11)
        pdf.multi_cell(0, 5.5, text, wrapmode="CHAR")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)
    pdf.output(output_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert a Markdown file into a readable PDF.")
    parser.add_argument("input", help="Path to input .md")
    parser.add_argument("output", help="Path to output .pdf")
    parser.add_argument("--title", default=None, help="PDF title (defaults to input filename)")
    args = parser.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        md_text = f.read()
    title = args.title or os.path.splitext(os.path.basename(args.input))[0]

    md_to_pdf(md_text=md_text, output_path=args.output, title=title)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
