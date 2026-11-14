#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""The help formatter of StreamWright's commands: descriptions wrap to the terminal, examples keep their lines."""

import argparse
import textwrap

__all__ = ["DescriptionFormatter", "sentence"]


class DescriptionFormatter(argparse.HelpFormatter):
    """
    Wraps each paragraph of a description or epilog (blank-line separated) to the terminal's width; a paragraph
    with an indented line (examples, tables) keeps its lines as written.
    """

    def _fill_text(self, text, width, indent):
        paragraphs = []
        for paragraph in text.strip("\n").split("\n\n"):
            lines = paragraph.splitlines()
            if any(line[:1].isspace() for line in lines):
                paragraphs.append("\n".join(indent + line for line in lines))
            else:
                paragraphs.append(textwrap.fill(" ".join(line.strip() for line in lines), width,
                                                initial_indent=indent, subsequent_indent=indent))
        return "\n\n".join(paragraphs)


def sentence(text):
    """A help line (`list the batches`) as a description's sentence (`List the batches.`)."""
    text = text.strip()
    return text[:1].upper() + text[1:] + ("" if text.endswith(".") else ".")
