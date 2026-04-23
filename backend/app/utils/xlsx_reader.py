from __future__ import annotations

from pathlib import Path
import zipfile
import xml.etree.ElementTree as ET


MAIN_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_NS = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
OFFICE_REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"


class XlsxReaderError(ValueError):
    """xlsx 原始读取异常。"""


def _column_ref_to_index(cell_ref: str) -> int:
    letters = "".join(char for char in cell_ref if char.isalpha())
    if not letters:
        return 0

    index = 0
    for char in letters:
        index = index * 26 + (ord(char.upper()) - 64)
    return index - 1


def _load_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []

    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for shared_string in root.findall("a:si", MAIN_NS):
        text_parts: list[str] = []
        for node in shared_string.iter():
            if node.tag == "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t":
                text_parts.append(node.text or "")
        values.append("".join(text_parts))
    return values


def _get_sheet_target(archive: zipfile.ZipFile, sheet_name: str) -> str:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in relationships.findall("r:Relationship", REL_NS)
    }

    sheets = workbook.find("a:sheets", MAIN_NS)
    if sheets is None:
        raise XlsxReaderError("xlsx 文件缺少 sheets 定义")

    for sheet in sheets:
        if sheet.attrib.get("name") != sheet_name:
            continue

        relation_id = sheet.attrib.get(OFFICE_REL_NS)
        if not relation_id or relation_id not in rel_map:
            break

        target = rel_map[relation_id]
        return target if target.startswith("xl/") else f"xl/{target}"

    raise XlsxReaderError(f"未找到工作表：{sheet_name}")


def _read_sheet_rows(archive: zipfile.ZipFile, sheet_target: str) -> list[list[str]]:
    shared_strings = _load_shared_strings(archive)
    root = ET.fromstring(archive.read(sheet_target))
    sheet_data = root.find("a:sheetData", MAIN_NS)
    if sheet_data is None:
        return []

    rows: list[list[str]] = []
    for row in sheet_data.findall("a:row", MAIN_NS):
        cell_values: dict[int, str] = {}
        for cell in row.findall("a:c", MAIN_NS):
            index = _column_ref_to_index(cell.attrib.get("r", ""))
            cell_type = cell.attrib.get("t")
            value = ""
            if cell_type == "inlineStr":
                text_node = cell.find("a:is/a:t", MAIN_NS)
                value = "" if text_node is None else (text_node.text or "")
            else:
                raw_node = cell.find("a:v", MAIN_NS)
                if raw_node is not None and raw_node.text is not None:
                    raw_value = raw_node.text
                    if cell_type == "s" and raw_value.isdigit():
                        shared_index = int(raw_value)
                        if 0 <= shared_index < len(shared_strings):
                            value = shared_strings[shared_index]
                        else:
                            value = raw_value
                    else:
                        value = raw_value
            cell_values[index] = value

        max_index = max(cell_values.keys(), default=-1)
        rows.append([cell_values.get(i, "") for i in range(max_index + 1)])

    return rows


def read_sheet_as_dicts(file_path: str | Path, sheet_name: str) -> list[dict[str, str]]:
    """将 xlsx 工作表按首行表头读取为字典列表。"""
    source_path = Path(file_path)
    with zipfile.ZipFile(source_path) as archive:
        sheet_target = _get_sheet_target(archive, sheet_name)
        rows = _read_sheet_rows(archive, sheet_target)

    if not rows:
        return []

    headers = [header.strip() for header in rows[0]]
    records: list[dict[str, str]] = []
    for row in rows[1:]:
        if not any(str(cell).strip() for cell in row):
            continue

        padded_row = row + [""] * max(0, len(headers) - len(row))
        record: dict[str, str] = {}
        for index, header in enumerate(headers):
            if not header:
                continue
            record[header] = str(padded_row[index]).strip()
        records.append(record)

    return records
